//! The session task: the one owner of the [`Session`].
//!
//! It runs passes. A pass offers again whatever the session could not
//! finish (the control stream first), ticks when the deadline has come,
//! moves the session's actions to the writer, hands its events to the
//! application, and lets the application act. Passes repeat while one made
//! progress; then the task waits for transport input, for room in the
//! writer's queue, or for the session's deadline.
//!
//! Who holds a read is always settled:
//! - the session took it: the reading task is told, and reads on;
//! - the session kept it and has not finished: nothing more of that stream
//!   is read until the empty resume call completes;
//! - the session did not admit it: it kept nothing, so the read stays here,
//!   whole, and is offered again.
//!
//! What is held is bounded: one read for each stream this side reads, whose
//! number the transport limits, and a fixed number of actions on the way to
//! the writer. When that queue is full the session is not polled.
//!
//! The session's close is the exception. It supersedes everything still
//! queued, so it is polled whatever waits here and goes to the writer on a
//! path of its own: neither a full queue nor a writer the transport is
//! making wait can hold it.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use moq5_core::{
    ActionPoll, Error, Event, Fed, FeedError, Instant, LocationFilter, Namespace, OwedSignal,
    OwnedActionDetail, Retirement, Session, SignalError, StreamRef, SubscribeConfig, Subscription,
    Tick, UniRoute,
};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};

use crate::transport::{self, Close, Ended, Input, Output};

const CLOSE_NO_ERROR: u64 = 0x0;
const CLOSE_INTERNAL_ERROR: u64 = 0x1;
const CLOSE_PROTOCOL_VIOLATION: u64 = 0x3;
/// The longest stream type: these drafts' variable-length integers take at
/// most nine bytes.
const TYPE_MAX: usize = 9;

type Fail = Box<dyn std::error::Error>;

/// What to subscribe to.
pub struct Want {
    pub namespace: Namespace,
    pub track: String,
    /// Unsubscribe after this many objects; without it, read until the
    /// publisher completes the subscription.
    pub objects: Option<u64>,
}

/// How the run went.
#[derive(Debug, Default)]
pub struct Summary {
    pub objects: u64,
    /// The publisher completed the subscription.
    pub completed: bool,
    /// The wanted number of objects arrived and this side unsubscribed.
    pub enough: bool,
    /// The subscription was refused.
    pub refused: bool,
    /// How the session ended.
    pub ending: Option<Ending>,
    /// Reads the session kept and finished later, reads it did not admit,
    /// and peer signals it owed: how often the host had to hold something.
    pub kept_reads: u64,
    pub refused_reads: u64,
    pub owed_signals: u64,
}

impl Summary {
    /// Whether the run did what it was asked: the subscription ended as
    /// wanted, and the session was then closed without an error, by this
    /// side or by the peer's application. A connection that ended any
    /// other way carries no termination code, and is a failure whatever
    /// had arrived before.
    pub fn succeeded(&self) -> bool {
        let clean = matches!(
            self.ending,
            Some(Ending::Closed { code: 0, .. })
                | Some(Ending::Connection(Ended::PeerApplication { code: 0, .. }))
        );
        (self.completed || self.enough) && clean
    }
}

/// How the session ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ending {
    /// This side closed it, with this termination code.
    Closed { code: u64, reason: Vec<u8> },
    /// The connection ended first.
    Connection(Ended),
}

impl fmt::Display for Ending {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Ending::Closed { code, reason } => {
                write!(f, "closed {}", transport::coded(code, reason))
            }
            Ending::Connection(ended) => ended.fmt(f),
        }
    }
}

/// What a stream this side reads carries.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A peer stream whose type has not arrived whole.
    Unrouted,
    Control,
    Data,
    Padding,
    /// The answering half of a request stream this side opened.
    Request,
}

/// One read of a stream, and the reading task waiting on it.
struct Read {
    bytes: Bytes,
    fin: bool,
    taken: oneshot::Sender<()>,
}

/// A stream this side reads.
struct Reading {
    kind: Kind,
    /// The bytes of an unrouted stream so far.
    leading: Vec<u8>,
    stop: Option<oneshot::Sender<u64>>,
    /// A read the session did not admit, kept whole to be offered again.
    refused: Option<Read>,
    /// The session kept a read and has not finished with it: the reading
    /// task waiting on it, and whether the read carried the FIN.
    kept: Option<(oneshot::Sender<()>, bool)>,
    /// Reading was stopped, and the session has not released the stream yet.
    retiring: bool,
}

pub struct Host {
    session: Session,
    epoch: std::time::Instant,
    input: mpsc::Receiver<Input>,
    output: mpsc::Sender<Output>,
    /// Output the writer had no room for, oldest first.
    waiting: VecDeque<Output>,
    /// Where the session's close goes.
    closing: Option<oneshot::Sender<Close>>,
    reading: HashMap<StreamRef, Reading>,
    /// The peer's control stream, and this side's.
    peer_control: Option<StreamRef>,
    own_control: Option<StreamRef>,
    /// Peer signals the session recorded but could not finish.
    owed: Vec<OwedSignal>,
    want: Want,
    established: bool,
    subscription: Option<Subscription>,
    unsubscribed: bool,
    /// The publisher completed the subscription before this side could
    /// unsubscribe; the completion is still to be reported.
    completing: bool,
    summary: Summary,
}

impl Host {
    pub fn new(
        session: Session,
        epoch: std::time::Instant,
        input: mpsc::Receiver<Input>,
        output: mpsc::Sender<Output>,
        closing: oneshot::Sender<Close>,
        want: Want,
    ) -> Host {
        Host {
            session,
            epoch,
            input,
            output,
            waiting: VecDeque::new(),
            closing: Some(closing),
            reading: HashMap::new(),
            peer_control: None,
            own_control: None,
            owed: Vec::new(),
            want,
            established: false,
            subscription: None,
            unsubscribed: false,
            completing: false,
            summary: Summary::default(),
        }
    }

    /// The host's clock: microseconds since `epoch`.
    fn now(&self) -> Instant {
        Instant::from_micros(self.epoch.elapsed().as_micros() as u64)
    }

    /// Drive the session until it has closed. By then its close, if it has
    /// one for the transport, is with the writer.
    pub async fn run(mut self) -> Result<Summary, Fail> {
        self.session.start(self.now())?;
        loop {
            while self.pass()? {}
            if self.session.is_closed() {
                break;
            }
            let deadline = self.until_deadline();
            let output = self.output.clone();
            tokio::select! {
                input = self.input.recv() => match input {
                    Some(input) => self.take(input)?,
                    None => break,
                },
                room = output.reserve(), if !self.waiting.is_empty() => {
                    let Ok(room) = room else { break };
                    if let Some(output) = self.waiting.pop_front() {
                        room.send(output);
                    }
                }
                () = tokio::time::sleep(deadline) => {}
            }
        }
        Ok(self.summary)
    }

    fn until_deadline(&self) -> Duration {
        match self.session.next_deadline() {
            Some(deadline) => deadline
                .saturating_duration_since(self.now())
                .max(Duration::from_millis(1)),
            None => Duration::from_secs(3600),
        }
    }

    /// One pass. Whether it made progress. A closed session takes no input
    /// and no time; its close and its last event are still polled.
    fn pass(&mut self) -> Result<bool, Fail> {
        let mut progress = false;
        if !self.session.is_closed() {
            progress |= self.retry()?;
            progress |= self.tick()?;
        }
        progress |= self.write_out()?;
        progress |= self.events()?;
        progress |= self.act()?;
        Ok(progress)
    }

    // -- Input -------------------------------------------------------------

    /// Take one message from the transport tasks.
    fn take(&mut self, input: Input) -> Result<(), Fail> {
        if self.session.is_closed() {
            // Nothing reaches a closed session. Dropping a read releases
            // its reading task, which ends.
            return Ok(());
        }
        let now = self.now();
        match input {
            Input::Opened { stream, stop } => {
                let kind = if stream.is_local() {
                    Kind::Request
                } else {
                    Kind::Unrouted
                };
                let reading = Reading {
                    kind,
                    leading: Vec::new(),
                    stop: Some(stop),
                    refused: None,
                    kept: None,
                    retiring: false,
                };
                self.reading.insert(stream, reading);
            }
            Input::Bytes {
                stream,
                bytes,
                fin,
                taken,
            } => self.read(stream, Read { bytes, fin, taken })?,
            Input::Reset { stream, code } => {
                let kind = self.ended(stream);
                let signal = match kind {
                    Some(Kind::Control) => {
                        self.session.on_control_stream_ended(now)?;
                        return Ok(());
                    }
                    Some(Kind::Data) => self.session.on_data_reset(stream, code, now),
                    Some(Kind::Request) => self.session.on_bidi_reset(stream, code, now),
                    Some(Kind::Unrouted | Kind::Padding) | None => Ok(()),
                };
                self.signalled(signal)?;
            }
            Input::Stopped { stream, code } => {
                if self.own_control == Some(stream) {
                    self.session.on_control_stream_ended(now)?;
                } else {
                    let signal = self.session.on_bidi_stop(stream, code, now);
                    self.signalled(signal)?;
                }
            }
            // The stream may still have output queued. Its retirement is
            // reported once the writer has passed this marker.
            Input::ReadStopped { stream } => self.queue(Output::Flush(stream))?,
            Input::Flushed { stream } => {
                if let Some(reading) = self.reading.get_mut(&stream) {
                    reading.retiring = true;
                }
            }
            Input::Datagram(bytes) => match self.session.on_datagram(&bytes, now) {
                // A datagram the session does not admit is dropped.
                Ok(()) | Err(FeedError::NotAdmitted(_)) => {}
                Err(FeedError::Failed(error)) => return Err(error.into()),
            },
            Input::Closed(ended) => {
                // The session is told the peer's termination code. Any other
                // end has none: the session gets zero for it, and what
                // happened is kept here, where success is decided.
                let code = match ended {
                    Ended::PeerApplication { code, .. } => code,
                    _ => 0,
                };
                self.summary.ending = Some(Ending::Connection(ended));
                self.session.on_transport_close(code, now)?;
            }
            Input::WriteFailed(why) => {
                eprintln!("moq5-quinn-subscriber: {why}");
                self.session
                    .close(CLOSE_INTERNAL_ERROR, "output failed", now)?;
            }
        }
        Ok(())
    }

    /// A new read. A peer stream is routed by its leading bytes first; the
    /// session says where it belongs, and this host never parses a type.
    fn read(&mut self, stream: StreamRef, mut read: Read) -> Result<(), Fail> {
        let Some(reading) = self.reading.get_mut(&stream) else {
            return Ok(());
        };
        if reading.kind == Kind::Unrouted {
            reading.leading.extend_from_slice(&read.bytes);
            reading.kind = match self.session.route_peer_uni(&reading.leading)? {
                UniRoute::NeedMore => {
                    if read.fin && reading.leading.is_empty() {
                        // Ended before any byte: that only ends it.
                        self.reading.remove(&stream);
                    } else if read.fin || reading.leading.len() >= TYPE_MAX {
                        self.violation("truncated stream type")?;
                    } else {
                        let _ = read.taken.send(());
                    }
                    return Ok(());
                }
                UniRoute::Control if self.peer_control.is_some() => {
                    return self.violation("second control stream");
                }
                UniRoute::Control => {
                    self.peer_control = Some(stream);
                    Kind::Control
                }
                UniRoute::Data => Kind::Data,
                UniRoute::Padding => Kind::Padding,
                _ => return self.violation("unknown stream type"),
            };
            // Every byte, the leading ones included, goes to the route's
            // input.
            read.bytes = Bytes::from(std::mem::take(&mut reading.leading));
        }
        self.deliver(stream, read, true)?;
        Ok(())
    }

    /// Offer one read to the session, for the first time or again, and
    /// settle who holds it. Whether the session took or kept it.
    fn deliver(&mut self, stream: StreamRef, read: Read, first: bool) -> Result<bool, Fail> {
        let now = self.now();
        let Some(reading) = self.reading.get_mut(&stream) else {
            return Ok(false);
        };
        let Read { bytes, fin, taken } = read;
        let fed = match reading.kind {
            // The peer finished its control stream.
            Kind::Control if fin => {
                self.session.on_control_stream_ended(now)?;
                return Ok(true);
            }
            Kind::Control => self.session.on_control_bytes(&bytes, now),
            Kind::Data => self.session.on_data_bytes(stream, &bytes, fin, now),
            Kind::Request => self.session.on_bidi_bytes(stream, &bytes, fin, now),
            Kind::Padding | Kind::Unrouted => Ok(Fed::Accepted),
        };
        match fed {
            Ok(Fed::Accepted) => {
                let _ = taken.send(());
                if fin {
                    self.ended(stream);
                }
                Ok(true)
            }
            // Kept by the session, which has more to do with it once its
            // output drains: resumed in `retry`.
            Ok(Fed::Pending) => {
                reading.kept = Some((taken, fin));
                self.summary.kept_reads += 1;
                Ok(true)
            }
            Err(FeedError::NotAdmitted(_)) => {
                reading.refused = Some(Read { bytes, fin, taken });
                self.summary.refused_reads += u64::from(first);
                Ok(false)
            }
            Err(FeedError::Failed(Error::CLOSED)) => Ok(false),
            Err(FeedError::Failed(error)) => Err(error.into()),
        }
    }

    /// The stream ended by itself: the session took its FIN, or the peer
    /// reset it. What it carried. If the session had asked to stop it, that
    /// end was read first and settles the stream: nothing is left to
    /// retire.
    fn ended(&mut self, stream: StreamRef) -> Option<Kind> {
        let reading = self.reading.remove(&stream)?;
        if reading.stop.is_none() {
            println!("ended {stream:?}");
        }
        Some(reading.kind)
    }

    /// A peer signal was delivered: keep it if the session still owes its
    /// handling.
    fn signalled(&mut self, signal: Result<(), SignalError>) -> Result<(), Fail> {
        match signal {
            Ok(()) | Err(SignalError::Failed(Error::CLOSED)) => Ok(()),
            Err(SignalError::Owed(owed)) => {
                self.owed.push(owed);
                self.summary.owed_signals += 1;
                Ok(())
            }
            Err(SignalError::Failed(error)) => Err(error.into()),
        }
    }

    fn violation(&mut self, why: &str) -> Result<(), Fail> {
        let now = self.now();
        self.session.close(CLOSE_PROTOCOL_VIOLATION, why, now)?;
        Ok(())
    }

    /// Offer again what the session could not finish, the control stream
    /// first.
    fn retry(&mut self) -> Result<bool, Fail> {
        let now = self.now();
        let mut progress = false;
        for owed in std::mem::take(&mut self.owed) {
            match self.session.redeliver(owed, now) {
                Ok(_) | Err(SignalError::Failed(Error::CLOSED)) => progress = true,
                Err(SignalError::Owed(owed)) => self.owed.push(owed),
                Err(SignalError::Failed(error)) => return Err(error.into()),
            }
        }
        let mut streams: Vec<StreamRef> = self.reading.keys().copied().collect();
        streams.sort_by_key(|stream| Some(*stream) != self.peer_control);
        for stream in streams {
            let Some(reading) = self.reading.get_mut(&stream) else {
                continue;
            };
            let kind = reading.kind;
            if let Some((taken, fin)) = reading.kept.take() {
                let fed = match kind {
                    Kind::Control => self.session.process_pending(now)?,
                    Kind::Data => self.session.resume_stream(stream, now)?,
                    _ => self.session.resume_bidi(stream, now)?,
                };
                if fed == Fed::Pending {
                    reading.kept = Some((taken, fin));
                } else {
                    let _ = taken.send(());
                    progress = true;
                    if fin {
                        self.ended(stream);
                        continue;
                    }
                }
            }
            let Some(reading) = self.reading.get_mut(&stream) else {
                continue;
            };
            if kind != Kind::Data || self.session.can_admit_data_stream() {
                if let Some(read) = reading.refused.take() {
                    progress |= self.deliver(stream, read, false)?;
                }
            }
            // A stream is retired only when no read of it is held here or
            // unfinished in the session.
            let free = |r: &Reading| r.retiring && r.kept.is_none() && r.refused.is_none();
            if self.reading.get(&stream).is_some_and(free) {
                progress |= self.retire(stream, kind)?;
            }
        }
        Ok(progress)
    }

    /// Report that a stream this side stopped reading will deliver nothing
    /// more. The session may not be ready to let it go yet; then this is
    /// repeated after the next poll.
    fn retire(&mut self, stream: StreamRef, kind: Kind) -> Result<bool, Fail> {
        let now = self.now();
        let retired = match kind {
            Kind::Data => self.session.on_data_stream_retired(stream, now)?,
            Kind::Request => self.session.on_bidi_stream_retired(stream, now)?,
            _ => Retirement::NotHeld,
        };
        if retired == Retirement::NotYet {
            return Ok(false);
        }
        self.reading.remove(&stream);
        println!("retired {stream:?}");
        Ok(true)
    }

    // -- Time --------------------------------------------------------------

    fn tick(&mut self) -> Result<bool, Fail> {
        let now = self.now();
        match self.session.next_deadline() {
            Some(deadline) if deadline <= now => Ok(matches!(self.session.tick(now)?, Tick::Done)),
            _ => Ok(false),
        }
    }

    // -- Output ------------------------------------------------------------

    /// Move the session's actions to the writer, in poll order, until the
    /// session has none or the writer's queue is full. A closed session has
    /// one action left, its close, and that is polled whatever waits.
    fn write_out(&mut self) -> Result<bool, Fail> {
        let mut progress = false;
        while self.waiting.is_empty() || self.session.is_closed() {
            // A polled action borrows the session; the writer gets a copy.
            let copied = match self.session.poll_action()? {
                ActionPoll::Ready(action) => action.into_owned().map_err(|(_, error)| error),
                _ => break,
            };
            progress = true;
            let action = match copied {
                Ok(action) => action,
                // A close that cannot be copied cannot be handed on.
                Err(error) if self.session.is_closed() => return Err(error.into()),
                Err(_) => {
                    let now = self.now();
                    self.session
                        .close(CLOSE_INTERNAL_ERROR, "output copy failed", now)?;
                    continue;
                }
            };
            match action.detail() {
                OwnedActionDetail::CloseSession { code, reason } => {
                    self.waiting.clear();
                    if let Some(closing) = self.closing.take() {
                        let _ = closing.send(Close {
                            code: *code,
                            reason: reason.clone(),
                        });
                    }
                    continue;
                }
                OwnedActionDetail::OpenUniControl { stream, .. } => {
                    self.own_control = Some(*stream);
                }
                // Stopping a stream is its reading task's to do. Its answer
                // (`Input::ReadStopped`) starts the stream's retirement.
                OwnedActionDetail::StopData { stream, error_code }
                | OwnedActionDetail::StopBidiStream { stream, error_code } => {
                    self.stop_reading(*stream, *error_code);
                    continue;
                }
                // An abort stops the reading half too; the writer resets the
                // other.
                OwnedActionDetail::AbortBidiStream { stream, error_code } => {
                    self.stop_reading(*stream, *error_code);
                }
                _ => {}
            }
            self.queue(Output::Action(action))?;
        }
        Ok(progress)
    }

    fn stop_reading(&mut self, stream: StreamRef, code: u64) {
        if let Some(stop) = self
            .reading
            .get_mut(&stream)
            .and_then(|reading| reading.stop.take())
        {
            println!("stopping {stream:?}");
            let _ = stop.send(code);
        }
    }

    /// Hand output to the writer without waiting; keep it when the writer's
    /// queue is full, behind whatever is already kept.
    fn queue(&mut self, output: Output) -> Result<(), Fail> {
        if !self.waiting.is_empty() {
            self.waiting.push_back(output);
            return Ok(());
        }
        match self.output.try_send(output) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(output)) => {
                self.waiting.push_back(output);
                Ok(())
            }
            Err(TrySendError::Closed(_)) => Err("the writer has ended".into()),
        }
    }

    // -- The application ---------------------------------------------------

    /// Hand the session's events to the application. An event borrows the
    /// session, so what the application does in answer waits for `act`.
    fn events(&mut self) -> Result<bool, Fail> {
        let mut progress = false;
        while let Some(event) = self.session.poll_event()? {
            progress = true;
            match event {
                Event::SetupComplete(_) => {
                    println!("setup complete");
                    self.established = true;
                }
                Event::SubscribeOk(ok) => println!("subscribed alias {}", ok.track_alias),
                Event::SubscribeError(refused) => {
                    println!(
                        "refused {}",
                        transport::coded(refused.error_code, refused.reason)
                    );
                    self.summary.refused = true;
                }
                Event::ObjectReceived(object) if !self.summary.enough => {
                    let bytes = object.payload.as_ref().map_or(0, |payload| payload.len());
                    println!(
                        "object {} {} {bytes} bytes",
                        object.group_id, object.object_id
                    );
                    self.summary.objects += 1;
                    self.summary.enough = Some(self.summary.objects) == self.want.objects;
                }
                // The peer gave up a subgroup's stream: the objects it had
                // not carried yet are not coming.
                Event::SubgroupReset(ended) if !self.summary.enough => println!(
                    "subgroup {} {} reset {}",
                    ended.group_id,
                    ended.subgroup_id,
                    ended.error_code.unwrap_or_default()
                ),
                Event::SubscribeDone(done) => {
                    println!(
                        "done status {} streams {}",
                        done.status_code, done.stream_count
                    );
                    self.summary.completed = true;
                }
                // The connection's own end, if that is what closed the
                // session, was recorded when it came.
                Event::SessionClosed(closed) => {
                    let ending = self.summary.ending.get_or_insert(Ending::Closed {
                        code: closed.code,
                        reason: closed.reason.to_vec(),
                    });
                    println!("{ending}");
                }
                _ => {}
            }
        }
        Ok(progress)
    }

    /// The application's own steps: subscribe once the setup is complete;
    /// unsubscribe when enough objects have arrived; close when the
    /// subscription is over.
    fn act(&mut self) -> Result<bool, Fail> {
        if self.session.is_closed() || !self.established {
            return Ok(false);
        }
        let now = self.now();
        let Some(subscription) = self.subscription else {
            let config = SubscribeConfig::new(
                self.want.namespace.clone(),
                self.want.track.as_bytes(),
                LocationFilter::NextObject,
            );
            self.subscription = Some(self.session.subscribe(&config, now)?);
            return Ok(true);
        };
        let over = self.summary.completed || self.summary.refused;
        if self.summary.enough && !over && !self.unsubscribed && !self.completing {
            match self.session.unsubscribe(subscription, now) {
                Ok(()) => {
                    println!("unsubscribed");
                    self.unsubscribed = true;
                    return Ok(true);
                }
                // No room for it yet: the next pass tries again.
                Err(Error::WOULD_BLOCK) => {}
                // The publisher has already completed the subscription, and
                // nothing may follow that. Its completion is reported once
                // the streams it announced have been read.
                Err(Error::WRONG_STATE) => self.completing = true,
                Err(error) => return Err(error.into()),
            }
            return Ok(false);
        }
        // After an unsubscribe the session stops the streams of the
        // subscription that are still open. The session is closed once no
        // data stream is left and every stopped stream has been retired or
        // has ended by itself.
        let settled = !self
            .reading
            .values()
            .any(|r| r.kind == Kind::Data || r.stop.is_none());
        if over || (self.unsubscribed && settled) {
            self.session.close(CLOSE_NO_ERROR, "", now)?;
            return Ok(true);
        }
        Ok(false)
    }
}
