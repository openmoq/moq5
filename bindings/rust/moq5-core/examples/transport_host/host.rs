//! A transport host built on the safe session API alone, over the in-memory
//! transport in `wire.rs`.
//!
//! Each side drives its [`Session`] with the public inputs, polls and
//! transport-host operations. It routes peer unidirectional streams with
//! `route_peer_uni`, keeps refused input for redelivery, holds output the
//! transport cannot take yet with `Action::into_owned`, and reports
//! retirement and the end of a control stream with the calls made for
//! them.
//!
//! **Order.** Output with the same order key reaches the transport in poll
//! order. In the tagged mode the key is the stream (one key for control),
//! so streams overtake each other, and an owner's final message waits for
//! its fence: every data stream of that owner has its FIN or reset accepted,
//! no reset claim of it is outstanding and no datagram of it is held.
//! Without tags there are no fences, so every stream shares one key: output
//! is written in poll order. Datagrams are held apart in both modes. In the
//! tagged mode a data stream's reset (claimed, polled, or owed after a
//! no-reset outcome) is written once that stream's earlier bytes are.
//!
//! **A FETCH's request after a STOP.** A response stream tagged
//! RESET_FINAL_ON_STOP keeps that policy until its FIN or reset is accepted.
//! A STOP of it seen before then is recorded once: the FETCH's final
//! message on its request stream, held now or polled later, is replaced by
//! a reset of the request stream with CANCELLED, never written, its FIN
//! included. Where the session ends the request itself (REQUEST_ENDED, or a
//! reset of the request it queued), its reset stands and nothing is
//! replaced.
//!
//! **FIN reports.** In the tagged mode each FIN of a local data stream that
//! the transport accepts, after every earlier byte of that stream, is
//! reported to the session once (`report_data_fin`), as soon as the write
//! that carried it ends and before anything else reaches the session; a
//! refused or partial write reports nothing. A FIN accepted once the
//! session has closed is part of shutdown: the close already ended what the
//! report would end, so `CLOSED` is the expected answer there and a fault
//! anywhere else.
//!
//! **STOPs and teardown.** Nothing more is written on a stopped stream. A
//! data stream's reset is claimed only once the session has taken the
//! STOP, and then WAIVED, whatever the reset's first cause: it keeps that
//! cause's code and delivers exactly the bytes the transport took. A
//! request STOP is answered with a reset carrying its code only once the
//! session's output shows no reset of its own for the stream; a native
//! reset already queued keeps its code, and no second one follows. A STOP
//! of a stream whose STOP the host already took is counted and changes
//! nothing. A native reset or abort of a request stream drops the bytes
//! held for it and goes at once; an ordinary FIN waits behind them.
//!
//! **Bounds.** At most `OUT_SLOTS` held records: at the limit the side stops
//! polling actions, and the session's queues push back. One read is held
//! per peer stream, and at most `MAX_REFUSED` peer streams are pending: read
//! but not yet admitted by the session, whether still being routed (a
//! stream type split across reads) or refused for want of an entry. A new
//! peer stream is first read only while fewer are pending, one at a time,
//! so streams found together, or whose types arrive in pieces, stay within
//! the bound. Control is read and fed first in every pass and is not
//! counted against the limit.
//!
//! **Before setup on drafts 18 and 22** the peer's control stream is a
//! unidirectional stream like any other until its type is read, and early
//! object and request streams may fill the pending bound first: they wait
//! for a setup that only the unread control stream can bring. So while the
//! session is not yet established, learning a stream's type is kept apart
//! from admitting its data. Of every unidirectional stream past the bound
//! only a prefix is read, long enough for any type: at most `TYPE_PREFIX`
//! bytes, in as many reads as they take to arrive, each answered by
//! `route_peer_uni`. No stream waits for another to be classified, so one
//! whose type is incomplete, or that has sent nothing, delays no other, and
//! the control stream is found as soon as its own type is complete. A
//! stream read this way holds only that prefix and is not counted against
//! the pending bound; there are no more of them than the transport lets the
//! peer open (`MAX_STREAMS` here), so all of them together fit in one
//! `HOLD_CAP`. A stream whose type is still unknown is never refused: it
//! may be the control stream. Control found this way is exempt as always; a
//! data stream joins the pending streams if they have room by then. One
//! found while they are still full, and a request stream past the bound,
//! are refused: STOP_SENDING, and a reset of this side's half of a request
//! stream, both EXCESSIVE_LOAD; what was read of them is dropped, and the
//! session never sees them. Draft 22 lets an endpoint reset early object
//! and request streams it does not buffer; draft 18 names request streams
//! only, and buffering early object streams is a SHOULD there: this host
//! buffers `MAX_REFUSED` of them and refuses the rest rather than hold an
//! unbounded number or stall setup. Once established, no further stream is
//! read past the bound: control is known, a new stream waits unread as
//! above, and the transport's stream credit pushes back. A stream that
//! still holds a type prefix then completes its type the same way, and a
//! data stream among them keeps just that prefix until the pending streams
//! have room. Draft 16's control stream is bidirectional and known on
//! arrival, so none of this applies there.
//!
//! **Time.** A side is driven in wakes: each wake has one time, runs passes
//! at it, and ticks the session at most once, only when its deadline has
//! come. A tick that returned "terminal owed" is not repeated at the same
//! time while the deadline stays where it left it.
//!
//! **Progress** is a transition: bytes, a reset or a datagram the transport
//! took, an input or action taken, a report, a claim, a commit. A refused
//! write, a pending result or a blocked tick is not.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use moq5_core::{
    Action, ActionDetail, ActionPoll, ActionTag, Buffer, ConnectionTarget, DataEndOutcome,
    DataOpenOutcome, DataResetOffer, Event, Facts, Fed, FeedError, Instant, NotAdmittedReason,
    OrderKey, OutputOwner, OwedSignal, OwnedAction, OwnedActionDetail, Perspective, PrefixFact,
    ResetOfferPoll, ResetState, Retirement, Session, SessionConfig, SessionState, SignalError,
    StreamRef, Tick, TransportBinding, UniRoute, UriTarget, UriVerdict, Version,
};

use crate::wire::{Wire, PIPE_CAP};

/// Transport streams per pair over its whole life.
pub const MAX_STREAMS: usize = 64;
/// Undelivered input kept per stream: one read.
pub const HOLD_CAP: usize = 2048;
/// Output records held per side.
pub const OUT_SLOTS: usize = 32;
/// Peer streams read but not yet admitted by the session, per side.
pub const MAX_REFUSED: usize = 4;
/// The most read of a peer stream past the pending bound before its type is
/// known: the longest encoding of a stream type on drafts 18 and 22, whose
/// variable-length integers take one to nine bytes.
pub const TYPE_PREFIX: usize = 9;
// Every type prefix held at once, one for each stream the transport lets the
// peer open, fits in a single stream's hold.
const _: () = assert!(MAX_STREAMS * TYPE_PREFIX <= HOLD_CAP);
/// Datagrams held per side.
pub const DGRAM_HOLD: usize = 8;

/// The stream error an early stream refused at the pending bound carries
/// (drafts 18 and 22).
pub const EXCESSIVE_LOAD: u64 = moq5_core::sys::MOQ_STREAM_ERROR_EXCESSIVE_LOAD as u64;

/// Session termination codes the drafts share.
pub const CLOSE_INTERNAL_ERROR: u64 = 0x1;
pub const CLOSE_PROTOCOL_VIOLATION: u64 = 0x3;

/// Why a side stopped driving its session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// An action kind, route or flag this host does not know.
    Unsupported,
    /// An action poll failed.
    Poll,
    /// The transport ran out of streams.
    Streams,
    /// Output for a stream this side does not know.
    Order,
    /// A session call returned a result its contract does not list: the
    /// call, and the native code (0 for an outcome rather than an error).
    Outcome(&'static str, i32),
    /// An input failed in a way this host ends on.
    Input,
    /// An output could not be copied.
    NoMemory,
}

/// How a pair is built.
#[derive(Clone, Default)]
pub struct Options {
    pub max_events: [u32; 2],
    pub max_actions: [u32; 2],
    pub max_data_streams: [u32; 2],
    pub write_limit: usize,
    pub read_limit: usize,
    pub drop_after_stop: bool,
    /// Sides in the tagged host mode.
    pub tagged: [bool; 2],
    /// Sides that feed data streams as owned buffers (`on_data`).
    pub feed_buffers: [bool; 2],
    /// Sides that surface objects as chunks.
    pub streaming_objects: [bool; 2],
    /// Seeded fragmentation of every write and read.
    pub seed: Option<u64>,
    /// Bind the client with this target (authority, path and query) even
    /// where the profile does not require a binding.
    pub client_target: Option<(Vec<u8>, Vec<u8>)>,
    /// Bind the server with this URI policy even where the profile does not
    /// require a binding.
    pub server_policy: Option<SharedPolicy>,
    /// Sides taking part in cluster metadata: (Hop ID, relay cost).
    pub cluster: [Option<(u64, Option<u64>)>; 2],
    /// Sides that answer subscription updates themselves.
    pub defer_updates: [bool; 2],
    /// Further session selections per side, applied last.
    pub configure: [Option<Configure>; 2],
}

/// A change to a side's session configuration.
pub type Configure = fn(SessionConfig) -> SessionConfig;

/// A URI policy several pairs can share.
pub type SharedPolicy = std::sync::Arc<dyn Fn(UriTarget<'_>) -> UriVerdict + Send + Sync>;

/// What a side did, for tests to assert on.
#[derive(Clone, Debug, Default)]
pub struct Counters {
    pub need_more: u32,
    pub routed_control: u32,
    pub routed_data: u32,
    pub routed_padding: u32,
    pub ended_unrouted: u32,
    pub control_ended: u32,
    pub refused_inputs: u32,
    pub redelivered: u32,
    /// Redeliveries that offered the very buffer the session returned.
    pub same_buffer_redelivered: u32,
    pub owned_retries: u32,
    pub partial_writes: u32,
    pub writes_refused: u32,
    pub poll_waits: u32,
    pub held_records: u32,
    pub peer_fins: u32,
    pub peer_resets: u32,
    pub peer_stops: u32,
    pub owed_signals: u32,
    pub data_retired: u32,
    pub bidi_retired: u32,
    pub retire_refused: u32,
    pub retire_stale: u32,
    pub violations: u32,
    pub opens_reported: u32,
    /// Accepted FINs reported to the session, and those accepted after it
    /// closed (shutdown).
    pub fins_reported: u32,
    pub fins_after_close: u32,
    pub claims: u32,
    pub claims_settled: u32,
    pub claim_waits: u32,
    pub commits: u32,
    pub host_resets: u32,
    pub held_expired: u32,
    pub final_waits: u32,
    pub ticks: u32,
    pub ticks_blocked: u32,
    pub datagrams_sent: u32,
    pub datagrams_dropped: u32,
    pub resets_unknown: u32,
    /// Response streams opened with RESET_FINAL_ON_STOP.
    pub final_policies: u32,
    /// Final messages replaced by a CANCELLED reset of their request.
    pub finals_cancelled: u32,
    /// Resets the transport refused, written again later.
    pub resets_refused: u32,
    /// Peer STOPs that reached the host, and those it had already taken.
    pub stop_entries: u32,
    pub stop_repeats: u32,
    /// Request STOPs this side answered with its own reset, after the
    /// session queued none.
    pub stops_answered: u32,
    /// Held writes a native bidi teardown discarded.
    pub teardown_discards: u32,
    /// New peer streams left unread at the pending-stream bound.
    pub admissions_deferred: u32,
    /// Streams read past the bound before setup, a prefix each, to learn
    /// their type; and the early streams refused at the bound.
    pub discoveries: u32,
    pub early_refused: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Unrouted,
    ControlBidi,
    ControlOut,
    ControlIn,
    DataOut,
    DataIn,
    Request,
    Padding,
}

/// A transport stream as this side knows it.
#[derive(Debug)]
struct Local {
    /// The transport stream; a local data stream is opened by its first
    /// accepted byte.
    wire: Option<usize>,
    stream: Option<StreamRef>,
    role: Role,
    peer_opened: bool,

    // Input: what this side read and the session has not taken.
    hold: Vec<u8>,
    hold_fin: bool,
    /// The buffer the session returned for redelivery (buffer feeds).
    returned: Option<Buffer>,
    refused: Option<NotAdmittedReason>,
    owned_retry: bool,
    owned_fin: bool,
    owed: Option<OwedSignal>,
    in_done: bool,
    stop_sent: bool,

    // Output.
    out_done: bool,
    drop_output: bool,
    peer_stop_done: bool,
    committed: bool,
    /// Bytes the transport took.
    written: u64,
    owner: Option<OutputOwner>,
    /// The first record's header: the prefix every reset still delivers.
    prefix: u64,
    open_pending: Option<OutputOwner>,
    end_known: bool,
    /// A reset this side writes once the stream's earlier bytes are.
    reset_owed: Option<(u64, u64)>,
    /// A FETCH response stream tagged RESET_FINAL_ON_STOP, and what a STOP
    /// of it seen while it was open made of its owner's request: the final
    /// message becomes a CANCELLED reset of the request stream, once,
    /// unless the session ends the request itself (`final_native`).
    reset_final_on_stop: bool,
    final_cancel: bool,
    final_native: bool,
    final_replaced: bool,
    /// A peer STOP of a request stream this side still answers with a reset
    /// of its half, once the session's own output for the stream is known:
    /// a native reset already queued keeps its code.
    stop_answer: Option<u64>,
    /// This side has begun to read the peer stream. A transport read is
    /// not an admission: the stream counts against the pending-stream bound
    /// until the session takes its first delivery.
    reading: bool,
    /// The session took the stream's first delivery: its entry exists.
    admitted: bool,
    /// Past the pending bound and read only to learn its type: it holds
    /// at most `TYPE_PREFIX` bytes, is fed nothing, and is not counted
    /// against the bound until it joins the pending streams.
    discovery: bool,
}

impl Local {
    fn new(wire: Option<usize>, stream: Option<StreamRef>, role: Role, peer_opened: bool) -> Local {
        Local {
            wire,
            stream,
            role,
            peer_opened,
            hold: Vec::new(),
            hold_fin: false,
            returned: None,
            refused: None,
            owned_retry: false,
            owned_fin: false,
            owed: None,
            in_done: false,
            stop_sent: false,
            out_done: false,
            drop_output: false,
            peer_stop_done: false,
            committed: false,
            written: 0,
            owner: None,
            prefix: 0,
            open_pending: None,
            end_known: false,
            reset_owed: None,
            reset_final_on_stop: false,
            final_cancel: false,
            final_native: false,
            final_replaced: false,
            stop_answer: None,
            reading: false,
            admitted: false,
            discovery: false,
        }
    }
}

/// A held output record. It counts itself, so a test can see every one is
/// released once.
#[derive(Debug)]
struct Held {
    local: Option<usize>,
    action: OwnedAction,
    /// Bytes the transport took.
    sent: usize,
    /// Bytes still to write at most (a trim), and whether the FIN was
    /// dropped by it.
    limit: Option<usize>,
    deadline: Option<Instant>,
    final_for: Option<OutputOwner>,
    /// The record was a FETCH's final message a STOP replaced: it resets
    /// its stream with this code instead.
    cancel: Option<u64>,
    live: Arc<Mutex<i64>>,
}

impl Drop for Held {
    fn drop(&mut self) {
        *self.live.lock().unwrap_or_else(|e| e.into_inner()) -= 1;
    }
}

/// A tag this host can act on: no unknown bit, and RESET_FINAL_ON_STOP only
/// where the header puts it, on a stream-opening SEND_DATA of an owner.
fn tag_valid(tag: &ActionTag, detail: &ActionDetail<'_>) -> bool {
    tag.unknown_flags() == 0
        && (!tag.reset_final_on_stop()
            || (matches!(detail, ActionDetail::SendData { .. })
                && tag.first_of_stream()
                && tag.owner().is_some()))
}

/// The data stream `d` owes the replacement of `owner`'s final message: a
/// STOP met it open, the session did not end the request itself, and the
/// replacement has not been made.
fn owes_cancel(d: &Local, owner: OutputOwner) -> bool {
    d.role == Role::DataOut
        && d.owner == Some(owner)
        && d.final_cancel
        && !d.final_native
        && !d.final_replaced
}

/// Replace `held`, if it is the final message of a request whose response
/// stream owes it, by a CANCELLED reset of the request stream, once. A
/// reset the session queued for the request is not a final write and
/// stands.
fn cancel_final(locals: &mut [Local], n: &mut Counters, held: &mut Held) {
    let (Some(owner), Some(local)) = (held.final_for, held.local) else {
        return;
    };
    if held.cancel.is_some()
        || locals[local].role != Role::Request
        || write_parts(held.action.detail()).is_none()
    {
        return;
    }
    let Some(d) = locals.iter_mut().find(|d| owes_cancel(d, owner)) else {
        return;
    };
    d.final_replaced = true;
    held.cancel = Some(u64::from(moq5_core::sys::MOQ_STREAM_ERROR_CANCELLED));
    held.final_for = None;
    n.finals_cancelled += 1;
}

/// The bytes and FIN a held or polled stream write carries.
fn write_parts(detail: &OwnedActionDetail) -> Option<([&[u8]; 2], bool)> {
    Some(match detail {
        OwnedActionDetail::SendControl { bytes } => ([bytes, &[]], false),
        OwnedActionDetail::OpenUniControl { bytes, .. } => ([bytes, &[]], false),
        OwnedActionDetail::SendUniControl { bytes, fin, .. }
        | OwnedActionDetail::OpenBidiStream { bytes, fin, .. }
        | OwnedActionDetail::SendBidiStream { bytes, fin, .. } => ([bytes, &[]], *fin),
        OwnedActionDetail::CloseBidiStream { .. } => ([&[], &[]], true),
        OwnedActionDetail::SendData {
            header,
            payload,
            fin,
            ..
        } => (
            [
                header.as_bytes(),
                payload.as_ref().map_or(&[][..], Buffer::as_slice),
            ],
            *fin,
        ),
        _ => return None,
    })
}

fn borrowed_parts<'a>(detail: &'a ActionDetail<'_>) -> Option<([&'a [u8]; 2], bool)> {
    Some(match detail {
        ActionDetail::SendControl { bytes } => ([*bytes, &[]], false),
        ActionDetail::OpenUniControl { bytes, .. } => ([*bytes, &[]], false),
        ActionDetail::SendUniControl { bytes, fin, .. }
        | ActionDetail::OpenBidiStream { bytes, fin, .. }
        | ActionDetail::SendBidiStream { bytes, fin, .. } => ([*bytes, &[]], *fin),
        ActionDetail::CloseBidiStream { .. } => ([&[], &[]], true),
        ActionDetail::SendData {
            header,
            payload,
            fin,
            ..
        } => (
            [
                header.as_bytes(),
                payload.as_ref().map_or(&[][..], |p| p.as_bytes()),
            ],
            *fin,
        ),
        _ => return None,
    })
}

/// The bytes `sent..total` of the concatenation of `parts`.
fn remaining(parts: [&[u8]; 2], sent: usize, total: usize) -> [&[u8]; 2] {
    let first = parts[0].len();
    let a = &parts[0][sent.min(first)..total.min(first).max(sent.min(first))];
    let b_end = total.saturating_sub(first);
    let b = &parts[1][sent.saturating_sub(first).min(b_end)..b_end];
    [a, b]
}

/// A held datagram, with its tag.
#[derive(Debug)]
struct HeldDatagram {
    bytes: Vec<u8>,
    deadline: Option<Instant>,
    owner: Option<OutputOwner>,
}

/// The one claim this side holds.
#[derive(Debug)]
struct Claim {
    offer: DataResetOffer,
    local: usize,
    reliable: u64,
}

/// Work an action leaves for after its borrow of the session ends.
enum After {
    Nothing,
    Expire(usize),
    /// The session's own close: close the transport, drop what is held.
    Close(u64),
    /// An output could not be held: close the session with an internal error.
    Abandon,
}

/// One side: everything but the session itself.
#[derive(Debug)]
struct State {
    side: usize,
    tagged: bool,
    feed_buffers: bool,
    bidi_control: bool,
    locals: Vec<Local>,
    control_bidi: Option<usize>,
    control_out: Option<usize>,
    control_in: Option<usize>,
    next_peer: u64,
    out: BTreeMap<OrderKey, VecDeque<Held>>,
    dgrams: VecDeque<HeldDatagram>,
    claim: Option<Claim>,
    /// Local data streams whose FIN the transport accepted, in that order,
    /// still to be reported (tagged mode).
    fins_accepted: VecDeque<usize>,
    control_pending: bool,
    wire_closed: bool,
    peer_close_told: bool,
    fault: Option<Fault>,
    n: Counters,
    live: Arc<Mutex<i64>>,
    ticked: bool,
    tick_blocked: Option<(Instant, Option<Instant>)>,
    /// The last action poll found the session's output empty: no reset it
    /// queued is still to be polled.
    drained: bool,
    /// Most actions one pass polls (0: until the session has none).
    poll_budget: u32,
}

/// One side of the pair.
#[derive(Debug)]
pub struct Host {
    pub session: Session,
    st: State,
}

impl State {
    fn fail(&mut self, fault: Fault) {
        if self.fault.is_none() {
            self.fault = Some(fault);
        }
    }

    fn held_count(&self) -> usize {
        self.out.values().map(VecDeque::len).sum::<usize>()
    }

    fn key(&self, key: OrderKey) -> OrderKey {
        // Without tags every stream shares one order; datagrams stand apart.
        if self.tagged || key == OrderKey::Datagrams {
            key
        } else {
            OrderKey::Session
        }
    }

    fn find(&self, stream: StreamRef, roles: &[Role]) -> Option<usize> {
        self.locals
            .iter()
            .position(|l| l.stream == Some(stream) && roles.contains(&l.role))
    }

    fn find_wire(&self, wire: usize) -> Option<usize> {
        self.locals.iter().position(|l| l.wire == Some(wire))
    }

    fn holds_output_for(&self, local: usize) -> bool {
        self.out
            .values()
            .any(|queue| queue.iter().any(|held| held.local == Some(local)))
    }

    /// Bytes of a stream the transport took, the taken part of a record in
    /// progress included.
    fn accepted(&self, local: usize) -> u64 {
        self.locals[local].written
    }

    fn unwritten(&self, local: usize) -> bool {
        self.out.values().any(|queue| {
            queue.iter().any(|held| {
                held.local == Some(local)
                    && write_parts(held.action.detail()).is_some_and(|(parts, _)| {
                        held.sent < held.limit.unwrap_or(parts[0].len() + parts[1].len())
                    })
            })
        })
    }

    /// Stop writing a stream past its first `keep` bytes: held writes
    /// shrink and lose their FIN.
    fn trim(&mut self, local: usize, keep: u64) {
        let mut at = self.locals[local].written;
        for queue in self.out.values_mut() {
            for held in queue.iter_mut().filter(|h| h.local == Some(local)) {
                let Some((parts, _)) = write_parts(held.action.detail()) else {
                    continue;
                };
                let len = held.limit.unwrap_or(parts[0].len() + parts[1].len());
                let room = keep.saturating_sub(at) as usize;
                let limit = held.sent + room.min(len - held.sent);
                at += (limit - held.sent) as u64;
                held.limit = Some(limit);
            }
        }
    }

    fn holds_reset(&self, local: usize) -> bool {
        self.locals[local].reset_owed.is_some()
            || self.claim.as_ref().is_some_and(|c| c.local == local)
    }

    /// After a no-reset outcome: drop what is held, and reset the stream
    /// with `code` unless its end is already accepted or held.
    fn owe_reset(&mut self, local: usize, code: u64) {
        self.locals[local].end_known = true;
        self.trim(local, 0);
        if self.locals[local].out_done || self.holds_reset(local) {
            return;
        }
        self.locals[local].reset_owed = Some((code, 0));
        self.n.host_resets += 1;
    }

    /// A STOP of `owner`'s response stream owes the replacement of its
    /// final message.
    fn cancel_owed(&self, owner: OutputOwner) -> bool {
        self.locals.iter().any(|d| owes_cancel(d, owner))
    }

    /// A native reset or abort of a bidirectional stream: drop the writes
    /// still held for it, then reset this side's half (held as owed if the
    /// transport refuses it) and, for an abort, stop the peer's.
    fn teardown(&mut self, wire: &mut Wire, local: usize, code: u64, abort: bool) {
        let mut discarded = 0;
        for queue in self.out.values_mut() {
            let before = queue.len();
            queue.retain(|held| {
                held.local != Some(local)
                    || held.cancel.is_some()
                    || write_parts(held.action.detail()).is_none()
            });
            discarded += before - queue.len();
        }
        self.n.teardown_discards += u32::try_from(discarded).unwrap_or(u32::MAX);
        let side = self.side;
        let index = self.locals[local].wire;
        let l = &mut self.locals[local];
        l.drop_output = true;
        if !l.out_done && l.reset_owed.is_none() {
            match index {
                Some(index) if !wire.try_reset(index, side, code, 0) => {
                    self.n.resets_refused += 1;
                    self.locals[local].reset_owed = Some((code, 0));
                }
                _ => self.locals[local].out_done = true,
            }
        }
        if abort && !self.locals[local].stop_sent {
            if let Some(index) = index {
                wire.stop(index, side, code);
            }
            self.locals[local].stop_sent = true;
        }
    }

    fn final_waits(&self, owner: OutputOwner) -> bool {
        self.claim
            .as_ref()
            .is_some_and(|c| c.offer.owner() == Some(owner))
            || self
                .locals
                .iter()
                .any(|l| l.role == Role::DataOut && l.owner == Some(owner) && !l.out_done)
            || self.dgrams.iter().any(|d| d.owner == Some(owner))
    }

    fn local_for_output(&mut self, wire: &mut Wire, detail: &ActionDetail<'_>) -> Option<usize> {
        match detail {
            // Control goes on the one bidirectional control stream (draft
            // 16, which the client opens with its first bytes and the server
            // answers on), or on the unidirectional stream OPEN_UNI_CONTROL
            // opened (drafts 18 and 22), which every later control message
            // continues; SEND_UNI_CONTROL, if one is ever emitted, is taken
            // the same way.
            ActionDetail::SendControl { .. } | ActionDetail::SendUniControl { .. }
                if !self.bidi_control =>
            {
                self.control_out
            }
            ActionDetail::SendControl { .. } | ActionDetail::SendUniControl { .. } => {
                if let Some(local) = self.control_bidi {
                    return Some(local);
                }
                if self.side != 0 {
                    return None;
                }
                let index = wire.open(self.side, true)?;
                self.locals
                    .push(Local::new(Some(index), None, Role::ControlBidi, false));
                self.control_bidi = Some(self.locals.len() - 1);
                self.control_bidi
            }
            ActionDetail::OpenUniControl { stream, .. } => {
                self.locals
                    .push(Local::new(None, Some(*stream), Role::ControlOut, false));
                self.control_out = Some(self.locals.len() - 1);
                self.control_out
            }
            ActionDetail::OpenBidiStream { stream, .. } => {
                let index = wire.open(self.side, true)?;
                self.locals
                    .push(Local::new(Some(index), Some(*stream), Role::Request, false));
                Some(self.locals.len() - 1)
            }
            ActionDetail::SendData { stream, .. } => {
                if let Some(local) = self.find(*stream, &[Role::DataOut]) {
                    return Some(local);
                }
                self.locals
                    .push(Local::new(None, Some(*stream), Role::DataOut, false));
                Some(self.locals.len() - 1)
            }
            ActionDetail::SendBidiStream { stream, .. }
            | ActionDetail::CloseBidiStream { stream }
            | ActionDetail::ResetBidiStream { stream, .. }
            | ActionDetail::StopBidiStream { stream, .. }
            | ActionDetail::AbortBidiStream { stream, .. } => self.find(*stream, &[Role::Request]),
            ActionDetail::ResetData { stream, .. } => self.find(*stream, &[Role::DataOut]),
            ActionDetail::StopData { stream, .. } => {
                self.find(*stream, &[Role::DataIn, Role::Unrouted])
            }
            _ => None,
        }
    }

    /// The transport stream of a local, opened on first use.
    fn wire_of(&mut self, wire: &mut Wire, local: usize) -> Option<usize> {
        if let Some(index) = self.locals[local].wire {
            return Some(index);
        }
        let index = wire.open(self.side, false)?;
        self.locals[local].wire = Some(index);
        Some(index)
    }

    /// Offer the bytes `sent..total` of a write, then its FIN once the last
    /// byte is taken: how many bytes the transport took, and whether the
    /// write is complete.
    fn try_write(
        &mut self,
        wire: &mut Wire,
        local: usize,
        parts: [&[u8]; 2],
        fin: bool,
        sent: usize,
        total: usize,
    ) -> (usize, bool) {
        let Some(index) = self.wire_of(wire, local) else {
            self.fail(Fault::Streams);
            return (0, false);
        };
        let mut took = 0;
        if sent < total {
            took = wire.write(index, self.side, remaining(parts, sent, total));
            if took == 0 {
                self.n.writes_refused += 1;
                return (0, false);
            }
            self.locals[local].written += took as u64;
        }
        let mut complete = sent + took == total;
        if complete && fin {
            // After the last byte; a stream that refuses it keeps it owed.
            if wire.fin(index, self.side) {
                self.locals[local].out_done = true;
                if self.tagged && self.locals[local].role == Role::DataOut {
                    // Every earlier byte of the stream is the transport's:
                    // the accepted FIN is reported once, after this write.
                    self.fins_accepted.push_back(local);
                }
            } else {
                self.n.writes_refused += 1;
                complete = false;
            }
        }
        if !complete {
            self.n.partial_writes += 1;
        }
        (took, complete)
    }

    fn expired(&self, local: Option<usize>, deadline: Option<Instant>, now: Instant) -> bool {
        let Some(local) = local else { return false };
        let l = &self.locals[local];
        self.tagged
            && l.role == Role::DataOut
            && !l.drop_output
            && !l.out_done
            && deadline.is_some_and(|d| d <= now)
    }
}

impl Host {
    fn new(
        side: usize,
        version: Version,
        options: &Options,
        live: Arc<Mutex<i64>>,
        now: Instant,
    ) -> moq5_core::Result<Host> {
        let perspective = if side == 0 {
            Perspective::Client
        } else {
            Perspective::Server
        };
        let mut config = SessionConfig::new(perspective, version)
            .request_capacity(64)
            .streaming_objects(options.streaming_objects[side])
            .defer_subscription_updates(options.defer_updates[side]);
        if let Some((hop_id, cost)) = options.cluster[side] {
            config = config.cluster(hop_id, cost);
        }
        if let Some(configure) = options.configure[side] {
            config = configure(config);
        }
        if options.max_events[side] != 0 {
            config = config.max_events(options.max_events[side]);
        }
        if options.max_actions[side] != 0 {
            config = config.max_actions(options.max_actions[side]);
        }
        if options.max_data_streams[side] != 0 {
            config = config.max_data_streams(options.max_data_streams[side]);
        }
        let mut session = Session::try_new(&config, now)?;
        let required = !session.binding_requirements().is_empty();
        let binding = if side == 0 {
            match (&options.client_target, required) {
                (Some((authority, path)), _) => Some(TransportBinding::raw_quic_client(
                    ConnectionTarget::new(authority.clone(), path.clone()),
                    Facts::all_supported(),
                )),
                (None, true) => Some(TransportBinding::raw_quic_client(
                    ConnectionTarget::new("host.invalid", ""),
                    Facts::all_supported(),
                )),
                (None, false) => None,
            }
        } else {
            match (&options.server_policy, required) {
                (Some(policy), _) => {
                    let policy = std::sync::Arc::clone(policy);
                    Some(TransportBinding::raw_quic_server(
                        Box::new(move |target| policy(target)),
                        Facts::all_supported(),
                    ))
                }
                (None, true) => Some(TransportBinding::raw_quic_server(
                    Box::new(|_| UriVerdict::Accept),
                    Facts::all_supported(),
                )),
                (None, false) => None,
            }
        };
        if let Some(binding) = binding {
            session
                .bind_transport(binding)
                .map_err(moq5_core::Error::from)?;
        }
        if options.tagged[side] {
            session.enable_tagged_output()?;
        }
        Ok(Host {
            session,
            st: State {
                side,
                tagged: options.tagged[side],
                feed_buffers: options.feed_buffers[side],
                bidi_control: version == Version::DRAFT_16,
                locals: Vec::new(),
                control_bidi: None,
                control_out: None,
                control_in: None,
                next_peer: 0,
                out: BTreeMap::new(),
                dgrams: VecDeque::new(),
                claim: None,
                fins_accepted: VecDeque::new(),
                control_pending: false,
                wire_closed: false,
                peer_close_told: false,
                fault: None,
                n: Counters::default(),
                live,
                ticked: false,
                tick_blocked: None,
                drained: false,
                poll_budget: 0,
            },
        })
    }

    /// The session closes itself over a limit or a peer violation of this
    /// host's finding; its close action closes the transport when written.
    fn close(&mut self, code: u64, reason: &str, now: Instant) {
        if code == CLOSE_PROTOCOL_VIOLATION {
            self.st.n.violations += 1;
        }
        let _ = self.session.close(code, reason, now);
    }

    fn input_failed(&mut self, now: Instant) {
        if self.session.is_closed() {
            return;
        }
        self.st.fail(Fault::Input);
        self.close(CLOSE_INTERNAL_ERROR, "input failed", now);
    }

    // -- Output ---------------------------------------------------------

    /// Poll while the hold has room; offer each action directly unless
    /// an older action of its key is held, its owner's fence is closed, or
    /// its bytes are past their deadline; hold the rest with `into_owned`.
    fn pump(&mut self, wire: &mut Wire, now: Instant) -> bool {
        let mut progress = false;
        self.st.drained = false;
        let budget = match self.st.poll_budget {
            0 => 4 * OUT_SLOTS,
            n => n as usize,
        };
        for _ in 0..budget {
            if self.st.held_count() >= OUT_SLOTS || self.st.wire_closed {
                break;
            }
            if self.st.fault.is_some() && !self.session.is_closed() {
                break;
            }
            let action = match self.session.poll_action() {
                Ok(ActionPoll::Ready(action)) => action,
                Ok(ActionPoll::Empty) => {
                    self.st.drained = true;
                    break;
                }
                Ok(ActionPoll::Waiting) => {
                    self.st.n.poll_waits += 1;
                    break;
                }
                Err(_) => {
                    self.st.fail(Fault::Poll);
                    break;
                }
            };
            progress = true;
            if action
                .tag()
                .is_some_and(|tag| !tag_valid(tag, action.detail()))
            {
                self.st.fail(Fault::Unsupported);
                break;
            }
            let after = self.st.take(wire, action, now);
            match after {
                After::Nothing => {}
                After::Expire(local) => self.expire_stream(local, now),
                After::Close(code) => {
                    wire.closed[self.st.side] = Some(code);
                    self.st.wire_closed = true;
                    self.st.out.clear();
                    self.st.dgrams.clear();
                }
                After::Abandon => self.close(CLOSE_INTERNAL_ERROR, "output copy failed", now),
            }
            self.report_opens();
            self.report_fins();
        }
        progress
    }

    /// Report every counted open whose first byte the transport took.
    fn report_opens(&mut self) {
        while let Some((local, owner)) = self.st.pending_open_report() {
            self.report_open(local, owner);
        }
    }

    /// Report each FIN the transport accepted, once, in the order it took
    /// them. Before the session closed every result but `Ok` is a fault;
    /// after, the report is part of shutdown and `CLOSED` is expected.
    fn report_fins(&mut self) {
        while let Some(local) = self.st.fins_accepted.pop_front() {
            let Some(stream) = self.st.locals[local].stream else {
                continue;
            };
            let shutdown = self.session.is_closed();
            match self.session.report_data_fin(stream) {
                Ok(()) => self.st.n.fins_reported += 1,
                Err(moq5_core::Error::CLOSED) if shutdown => self.st.n.fins_after_close += 1,
                Err(e) => self.st.fail(Fault::Outcome("report_data_fin", e.code())),
            }
        }
    }

    fn report_open(&mut self, local: usize, owner: OutputOwner) {
        let Some(stream) = self.st.locals[local].stream else {
            return;
        };
        self.st.locals[local].open_pending = None;
        match self
            .session
            .report_data_open(stream, owner, DataOpenOutcome::Opened)
        {
            Ok(()) => self.st.n.opens_reported += 1,
            Err(moq5_core::Error::STALE_HANDLE) => {}
            Err(e) => self.st.fail(Fault::Outcome("report_data_open", e.code())),
        }
    }

    /// Held bytes of a subgroup past their deadline. With no end decided,
    /// the session says how the stream ends; with one decided, the bytes are
    /// trimmed to the prefix that end still delivers.
    fn expire_stream(&mut self, local: usize, now: Instant) {
        if self.st.locals[local].end_known {
            let keep = self.st.kept_prefix(local);
            self.st.trim(local, keep);
            return;
        }
        let Some(stream) = self.st.locals[local].stream else {
            return;
        };
        self.st.locals[local].end_known = true;
        match self.session.on_held_data_expired(stream, now) {
            Ok(end) => {
                self.st.n.held_expired += 1;
                match end.outcome {
                    DataEndOutcome::ResetFollows => {
                        let prefix = self.st.locals[local].prefix;
                        self.st.trim(local, prefix);
                    }
                    DataEndOutcome::NoReset { host_reset_code } => {
                        self.st.owe_reset(local, host_reset_code)
                    }
                    _ => self
                        .st
                        .fail(Fault::Outcome("on_held_data_expired outcome", 0)),
                }
            }
            Err(moq5_core::Error::CLOSED) => self.st.trim(local, 0),
            Err(e) => self
                .st
                .fail(Fault::Outcome("on_held_data_expired", e.code())),
        }
    }

    /// Write held output in its order, each key on its own.
    fn flush(&mut self, wire: &mut Wire, now: Instant) -> bool {
        let mut progress = false;
        for _ in 0..(OUT_SLOTS + MAX_STREAMS) {
            if self.st.wire_closed {
                break;
            }
            let mut moved = false;
            let keys: Vec<OrderKey> = self.st.out.keys().copied().collect();
            for key in keys {
                loop {
                    let front = self.st.flush_front(wire, key, now);
                    self.report_opens();
                    self.report_fins();
                    match front {
                        Front::Done => moved = true,
                        Front::Expire(local) => {
                            self.expire_stream(local, now);
                            moved = true;
                        }
                        Front::Blocked(took) => {
                            moved |= took;
                            break;
                        }
                        Front::Empty => break,
                        Front::Close(code) => {
                            wire.closed[self.st.side] = Some(code);
                            self.st.wire_closed = true;
                            moved = true;
                            break;
                        }
                    }
                }
                if self.st.wire_closed {
                    break;
                }
            }
            self.st.out.retain(|_, queue| !queue.is_empty());
            self.report_opens();
            progress |= moved;
            if !moved {
                break;
            }
        }
        if self.st.wire_closed {
            self.st.out.clear();
            self.st.dgrams.clear();
        }
        progress
    }

    fn flush_datagrams(&mut self, wire: &mut Wire, now: Instant) -> bool {
        let mut progress = false;
        let before = self.st.dgrams.len();
        self.st
            .dgrams
            .retain(|d| !d.deadline.is_some_and(|due| due <= now));
        progress |= self.st.dgrams.len() != before;
        while let Some(front) = self.st.dgrams.front() {
            match wire.send_datagram(self.st.side, &front.bytes) {
                None => {
                    self.st.n.writes_refused += 1;
                    break;
                }
                Some(true) => self.st.n.datagrams_sent += 1,
                Some(false) => self.st.n.datagrams_dropped += 1,
            }
            self.st.dgrams.pop_front();
            progress = true;
        }
        progress
    }

    /// The reset transaction (tagged mode): one claim at a time.
    fn resets(&mut self) -> bool {
        let mut progress = false;
        for _ in 0..8 {
            if self.st.claim.is_some() {
                break;
            }
            let offer = match self.session.offer_data_reset() {
                Ok(ResetOfferPoll::Offer(offer)) => offer,
                Ok(ResetOfferPoll::Empty) => break,
                Ok(ResetOfferPoll::Waiting) => {
                    self.st.n.claim_waits += 1;
                    break;
                }
                Err(moq5_core::Error::CLOSED) => break,
                Err(_) => {
                    self.st.fail(Fault::Poll);
                    break;
                }
            };
            if offer.state() == ResetState::Claimed {
                self.st.fail(Fault::Outcome("offer_data_reset claimed", 0)); // this host holds no claim
                break;
            }
            let local = self.st.find(offer.stream(), &[Role::DataOut]);
            let Some(mut prefix) = self.st.prefix_fact(&offer, local) else {
                // A STOP this side has seen but the session has not taken:
                // the claim waits for it.
                self.st.n.claim_waits += 1;
                break;
            };
            let mut claimed = self.session.claim_data_reset(&offer, prefix);
            if claimed == Err(moq5_core::Error::WRONG_STATE)
                && prefix == PrefixFact::Unopened
                && local.is_some()
            {
                // A FETCH or fill stream is never claimed UNOPENED: its
                // reset follows its header, written first.
                prefix = PrefixFact::Held;
                claimed = self.session.claim_data_reset(&offer, prefix);
            }
            let claim = match claimed {
                Ok(claim) => claim,
                Err(moq5_core::Error::STALE_HANDLE) => continue,
                Err(moq5_core::Error::WOULD_BLOCK) => {
                    self.st.n.claim_waits += 1;
                    break;
                }
                Err(e) => {
                    self.st.fail(Fault::Outcome("claim_data_reset", e.code()));
                    break;
                }
            };
            if claim.unknown_flags() != 0 {
                self.st.fail(Fault::Outcome("claim flags", 0));
                break;
            }
            self.st.n.claims += 1;
            progress = true;
            if claim.settled() {
                self.st.n.claims_settled += 1;
                if let Some(local) = local {
                    self.st.trim(local, 0);
                    let l = &mut self.st.locals[local];
                    l.open_pending = None;
                    l.out_done = true;
                    l.end_known = true;
                }
                continue;
            }
            let Some(local) = local else {
                self.st.fail(Fault::Order);
                break;
            };
            self.st.trim(
                local,
                if prefix == PrefixFact::Held {
                    offer.reliable_size()
                } else {
                    0
                },
            );
            self.st.locals[local].end_known = true;
            let reliable = if prefix == PrefixFact::Waived {
                self.st.accepted(local)
            } else {
                offer.reliable_size()
            };
            self.st.claim = Some(Claim {
                offer,
                local,
                reliable,
            });
        }
        progress
    }

    /// The claimed reset, once the prefix it delivers is written; committed
    /// once the transport takes it.
    fn claim_write(&mut self, wire: &mut Wire) -> bool {
        let Some(claim) = self.st.claim.as_ref() else {
            return false;
        };
        let local = claim.local;
        if self.st.wire_closed {
            self.st.claim = None;
            return false;
        }
        if !self.st.locals[local].drop_output && self.st.unwritten(local) {
            return false;
        }
        let (code, reliable) = (claim.offer.error_code(), claim.reliable);
        if !self.st.locals[local].out_done {
            if let Some(index) = self.st.locals[local].wire {
                if !wire.try_reset(index, self.st.side, code, reliable) {
                    // The claim stays; its reset is offered again.
                    self.st.n.resets_refused += 1;
                    return false;
                }
            }
            self.st.locals[local].out_done = true;
        }
        let Some(claim) = self.st.claim.take() else {
            return false;
        };
        match self.session.commit_data_reset(&claim.offer) {
            Ok(()) => self.st.n.commits += 1,
            Err(moq5_core::Error::CLOSED) => {}
            Err(e) => self.st.fail(Fault::Outcome("commit_data_reset", e.code())),
        }
        true
    }

    /// Resets this side writes on its own timing, each once its stream's
    /// earlier bytes are written.
    fn owed_resets(&mut self, wire: &mut Wire) -> bool {
        let mut progress = false;
        for local in 0..self.st.locals.len() {
            let Some((code, reliable)) = self.st.locals[local].reset_owed else {
                continue;
            };
            if self.st.locals[local].out_done {
                self.st.locals[local].reset_owed = None;
                continue;
            }
            if !self.st.locals[local].drop_output && self.st.unwritten(local) {
                continue;
            }
            if let Some(index) = self.st.locals[local].wire {
                if !wire.try_reset(index, self.st.side, code, reliable) {
                    self.st.n.resets_refused += 1;
                    continue;
                }
            }
            let l = &mut self.st.locals[local];
            l.out_done = true;
            l.reset_owed = None;
            progress = true;
        }
        progress
    }

    /// Answer the request STOPs the session left to this side: once its
    /// output is drained, a stream no native reset or teardown reached is
    /// reset with the STOP's code. Writes held for it were dropped by the
    /// STOP.
    fn answer_stops(&mut self, wire: &mut Wire) -> bool {
        if !self.st.drained || self.st.claim.is_some() {
            return false;
        }
        let mut progress = false;
        for local in 0..self.st.locals.len() {
            let Some(code) = self.st.locals[local].stop_answer else {
                continue;
            };
            let l = &self.st.locals[local];
            if l.out_done || l.reset_owed.is_some() {
                self.st.locals[local].stop_answer = None;
                continue;
            }
            if let Some(index) = l.wire {
                wire.reset(index, self.st.side, code, 0);
            }
            let l = &mut self.st.locals[local];
            l.out_done = true;
            l.stop_answer = None;
            self.st.n.stops_answered += 1;
            progress = true;
        }
        progress
    }

    // -- Input ----------------------------------------------------------

    fn feed(&mut self, local: usize, now: Instant) -> bool {
        let role = self.st.locals[local].role;
        match role {
            Role::ControlBidi | Role::ControlIn => {
                if self.st.control_pending {
                    return false;
                }
                if !self.st.locals[local].hold.is_empty() {
                    let bytes = std::mem::take(&mut self.st.locals[local].hold);
                    match self.session.on_control_bytes(&bytes, now) {
                        Ok(Fed::Accepted) => {}
                        Ok(Fed::Pending) => self.st.control_pending = true,
                        Err(_) => {
                            self.input_failed(now);
                            return false;
                        }
                    }
                }
                if self.st.locals[local].hold_fin {
                    let l = &mut self.st.locals[local];
                    l.hold_fin = false;
                    l.in_done = true;
                    self.st.n.peer_fins += 1;
                    self.st.n.control_ended += 1;
                    let _ = self.session.on_control_stream_ended(now);
                }
                true
            }
            Role::Padding => {
                let l = &mut self.st.locals[local];
                l.hold.clear();
                if l.hold_fin {
                    l.hold_fin = false;
                    l.in_done = true;
                }
                true
            }
            Role::DataIn | Role::Request => self.feed_stream(local, role, now),
            _ => false,
        }
    }

    fn feed_stream(&mut self, local: usize, role: Role, now: Instant) -> bool {
        let Some(stream) = self.st.locals[local].stream else {
            return false;
        };
        let fin = self.st.locals[local].hold_fin;
        let result: Result<Fed, Option<NotAdmittedReason>> =
            if role == Role::DataIn && self.st.feed_buffers {
                let returned = self.st.locals[local].returned.take();
                let redelivering = returned.is_some();
                let buffer = match returned {
                    Some(buffer) => buffer,
                    None => match Buffer::copy_from(&self.st.locals[local].hold) {
                        Ok(buffer) => buffer,
                        Err(_) => {
                            self.st.fail(Fault::NoMemory);
                            return false;
                        }
                    },
                };
                let raw = buffer.as_ptr();
                match self.session.on_data(stream, buffer, fin, now) {
                    Ok(fed) => {
                        if redelivering {
                            self.st.n.same_buffer_redelivered += 1;
                        }
                        Ok(fed)
                    }
                    Err(FeedError::NotAdmitted(refused)) => {
                        // The session hands back the very buffer it was given.
                        assert_eq!(refused.input.as_ptr(), raw, "the refused buffer comes back");
                        assert_eq!(refused.fin, fin);
                        let reason = refused.reason;
                        self.st.locals[local].returned = Some(refused.input);
                        Err(Some(reason))
                    }
                    Err(FeedError::Failed(_)) => Err(None),
                }
            } else {
                let bytes = std::mem::take(&mut self.st.locals[local].hold);
                let result = if role == Role::DataIn {
                    self.session.on_data_bytes(stream, &bytes, fin, now)
                } else {
                    self.session.on_bidi_bytes(stream, &bytes, fin, now)
                };
                let mapped = match result {
                    Ok(fed) => Ok(fed),
                    Err(FeedError::NotAdmitted(refused)) => {
                        assert!(
                            std::ptr::eq(refused.input, &bytes[..]),
                            "the same bytes come back"
                        );
                        Err(Some(refused.reason))
                    }
                    Err(FeedError::Failed(_)) => Err(None),
                };
                if mapped.is_err() {
                    // Nothing was taken: this side keeps the exact bytes.
                    self.st.locals[local].hold = bytes;
                }
                mapped
            };
        match result {
            Ok(fed) => {
                let l = &mut self.st.locals[local];
                l.admitted = true;
                l.hold.clear();
                l.hold_fin = false;
                if l.refused.take().is_some() {
                    self.st.n.redelivered += 1;
                }
                let l = &mut self.st.locals[local];
                if fed == Fed::Pending {
                    l.owned_retry = true;
                    l.owned_fin = fin;
                } else if fin {
                    l.in_done = true;
                    self.st.n.peer_fins += 1;
                }
                true
            }
            Err(Some(reason)) => {
                if self.st.locals[local].refused.is_none() {
                    self.st.locals[local].refused = Some(reason);
                    self.st.n.refused_inputs += 1;
                }
                false
            }
            Err(None) => {
                let l = &mut self.st.locals[local];
                l.hold.clear();
                l.hold_fin = false;
                l.returned = None;
                self.input_failed(now);
                false
            }
        }
    }

    fn refused_count(&self) -> usize {
        self.st
            .locals
            .iter()
            .filter(|l| l.refused == Some(NotAdmittedReason::NoEntry))
            .count()
    }

    /// Peer streams this side has begun to read whose first delivery the
    /// session has not taken: still being routed, routed but not yet fed,
    /// or refused. Control and padding need no admission.
    fn pending_count(&self) -> usize {
        self.st
            .locals
            .iter()
            .filter(|l| {
                l.peer_opened
                    && l.reading
                    && !l.admitted
                    && !l.in_done
                    && !l.discovery
                    && !matches!(l.role, Role::ControlBidi | Role::ControlIn | Role::Padding)
            })
            .count()
    }

    /// Before setup on a draft whose control stream is unidirectional: an
    /// unread stream may be the peer's control stream.
    fn early(&self) -> bool {
        !self.st.bidi_control
            && matches!(
                self.session.state(),
                SessionState::Idle | SessionState::SetupSent
            )
    }

    /// Refuse an early peer stream at the pending bound: STOP_SENDING, and
    /// a reset of this side's half of a request stream, both
    /// EXCESSIVE_LOAD. What was read of it is dropped; the session never
    /// saw it.
    fn refuse_early(&mut self, local: usize, wire: &mut Wire) {
        let side = self.st.side;
        let l = &mut self.st.locals[local];
        if let Some(index) = l.wire {
            wire.stop(index, side, EXCESSIVE_LOAD);
            if l.role == Role::Request {
                wire.reset(index, side, EXCESSIVE_LOAD, 0);
            }
        }
        l.stop_sent = true;
        l.out_done |= l.role == Role::Request;
        l.hold.clear();
        l.hold_fin = false;
        l.in_done = true;
        l.discovery = false;
        self.st.n.early_refused += 1;
    }

    fn route(&mut self, local: usize, now: Instant) -> Option<bool> {
        let route = match self.session.route_peer_uni(&self.st.locals[local].hold) {
            Ok(route) => route,
            Err(moq5_core::Error::CLOSED) => return None,
            Err(_) => {
                self.st.fail(Fault::Input);
                return None;
            }
        };
        let ended = self.st.locals[local].hold_fin;
        match route {
            UniRoute::NeedMore => {
                self.st.n.need_more += 1;
                let l = &mut self.st.locals[local];
                // A type is complete within its longest encoding; a stream
                // read only that far has no more room than that.
                let cap = if l.discovery { TYPE_PREFIX } else { HOLD_CAP };
                if ended && l.hold.is_empty() {
                    l.hold_fin = false;
                    l.in_done = true;
                    self.st.n.ended_unrouted += 1;
                } else if ended || l.hold.len() >= cap {
                    l.hold.clear();
                    l.hold_fin = false;
                    l.in_done = true;
                    self.close(CLOSE_PROTOCOL_VIOLATION, "truncated stream type", now);
                } else {
                    return Some(false); // waiting for more
                }
                None
            }
            UniRoute::Control => {
                if self.st.control_in.is_some() {
                    self.st.locals[local].in_done = true;
                    self.close(CLOSE_PROTOCOL_VIOLATION, "second control stream", now);
                    return None;
                }
                self.st.control_in = Some(local);
                self.st.locals[local].role = Role::ControlIn;
                self.st.n.routed_control += 1;
                Some(true)
            }
            UniRoute::Data => {
                self.st.locals[local].role = Role::DataIn;
                self.st.n.routed_data += 1;
                Some(true)
            }
            UniRoute::Padding => {
                self.st.locals[local].role = Role::Padding;
                self.st.n.routed_padding += 1;
                Some(true)
            }
            UniRoute::UnknownType => {
                self.st.locals[local].in_done = true;
                self.close(CLOSE_PROTOCOL_VIOLATION, "unknown stream type", now);
                None
            }
            _ => {
                self.st.fail(Fault::Unsupported);
                None
            }
        }
    }

    fn peer_reset(&mut self, local: usize, code: u64, now: Instant) -> bool {
        let l = &mut self.st.locals[local];
        l.hold.clear();
        l.hold_fin = false;
        l.returned = None;
        l.refused = None;
        let role = l.role;
        let stream = l.stream;
        let result = match (role, stream) {
            (Role::ControlBidi | Role::ControlIn, _) => {
                self.st.n.control_ended += 1;
                self.session
                    .on_control_stream_ended(now)
                    .map_err(SignalError::Failed)
            }
            (Role::Unrouted, _) => {
                self.st.locals[local].in_done = true;
                self.st.n.ended_unrouted += 1;
                return true;
            }
            (Role::DataIn, Some(stream)) => self.session.on_data_reset(stream, code, now),
            (Role::Request, Some(stream)) => self.session.on_bidi_reset(stream, code, now),
            (Role::Padding, _) => Ok(()),
            _ => {
                self.st.fail(Fault::Order);
                return false;
            }
        };
        self.settle_signal(local, result)
    }

    /// Record how the session took a peer signal; an owed one is delivered
    /// again on later passes, with its arguments.
    fn settle_signal(&mut self, local: usize, result: Result<(), SignalError>) -> bool {
        match result {
            Ok(()) | Err(SignalError::Failed(moq5_core::Error::CLOSED)) => {
                let l = &mut self.st.locals[local];
                l.owed = None;
                l.in_done = true;
                l.owned_retry = false;
                self.st.n.peer_resets += 1;
                true
            }
            Err(SignalError::Owed(owed)) => {
                if self.st.locals[local].owed.is_none() {
                    self.st.n.owed_signals += 1;
                }
                self.st.locals[local].owed = Some(owed);
                false
            }
            Err(SignalError::Failed(e)) => {
                self.st.fail(Fault::Outcome("peer signal", e.code()));
                false
            }
        }
    }

    /// A peer STOP reaching the host. A STOP of a stream whose STOP the
    /// host already took is counted and changes nothing: the obligations
    /// the first one recorded stand once.
    fn peer_stop_entry(&mut self, local: usize, code: u64, now: Instant) -> bool {
        self.st.n.stop_entries += 1;
        if self.st.locals[local].peer_stop_done {
            self.st.n.stop_repeats += 1;
            return true;
        }
        self.peer_stop(local, code, now)
    }

    fn peer_stop(&mut self, local: usize, code: u64, now: Instant) -> bool {
        self.st.locals[local].drop_output = true;
        let role = self.st.locals[local].role;
        let stream = self.st.locals[local].stream;
        let ok = match (role, stream) {
            (Role::ControlBidi | Role::ControlOut, _) => {
                self.st.n.control_ended += 1;
                self.session.on_control_stream_ended(now).is_ok()
            }
            (Role::DataOut, Some(stream)) => {
                // Only a STOP that finds the stream open ends a FETCH's
                // request too (RESET_FINAL_ON_STOP).
                let open = !self.st.locals[local].out_done;
                if let Some(claim) = self.st.claim.as_mut().filter(|c| c.local == local) {
                    // A held claim turns waived: its reset delivers only
                    // what the transport took.
                    claim.reliable = self.st.locals[local].written;
                    if open {
                        self.st.stop_final(local, false);
                    }
                    true
                } else {
                    match self.session.on_data_stop(stream, code, now) {
                        Ok(end) if end.unknown_flags != 0 => {
                            self.st.fail(Fault::Outcome("on_data_stop flags", 0));
                            false
                        }
                        Ok(end) => {
                            self.st.locals[local].end_known = true;
                            match end.outcome {
                                DataEndOutcome::NoReset { host_reset_code } => {
                                    self.st.owe_reset(local, host_reset_code);
                                }
                                DataEndOutcome::ResetFollows => {}
                                _ => self.st.fail(Fault::Outcome("on_data_stop outcome", 0)),
                            }
                            if open {
                                self.st.stop_final(local, end.request_ended);
                            }
                            true
                        }
                        Err(SignalError::Owed(_)) => false, // the same stop again
                        Err(SignalError::Failed(moq5_core::Error::CLOSED)) => true,
                        Err(SignalError::Failed(e)) => {
                            self.st.fail(Fault::Outcome("on_data_stop", e.code()));
                            false
                        }
                    }
                }
            }
            (Role::Request, Some(stream)) => {
                // The STOP is answered with a reset of this side's half,
                // once, but not before the session's own output for the
                // stream is known: a native reset it already queued keeps
                // its code, and no second one follows (`answer_stops`).
                match self.session.on_bidi_stop(stream, code, now) {
                    Ok(()) | Err(SignalError::Failed(moq5_core::Error::CLOSED)) => {
                        if !self.st.locals[local].out_done {
                            self.st.locals[local].stop_answer = Some(code);
                        }
                        true
                    }
                    Err(SignalError::Owed(_)) => false,
                    Err(SignalError::Failed(e)) => {
                        self.st.fail(Fault::Outcome("on_bidi_stop", e.code()));
                        false
                    }
                }
            }
            _ => true, // this side writes nothing there
        };
        if ok {
            self.st.locals[local].peer_stop_done = true;
            self.st.n.peer_stops += 1;
        }
        ok
    }

    fn retire(&mut self, local: usize, bidi: bool, now: Instant) -> bool {
        let l = &self.st.locals[local];
        if !l.stop_sent || !l.hold.is_empty() || l.hold_fin {
            return false;
        }
        let Some(stream) = l.stream else {
            return false;
        };
        let result = if bidi {
            if l.role != Role::Request {
                self.st.locals[local].in_done = true;
                return true;
            }
            if self.st.holds_output_for(local) {
                return false;
            }
            self.session.on_bidi_stream_retired(stream, now)
        } else {
            if l.role != Role::DataIn {
                self.st.locals[local].in_done = true;
                return true;
            }
            self.session.on_data_stream_retired(stream, now)
        };
        match result {
            Ok(Retirement::Released) => {
                if bidi {
                    self.st.n.bidi_retired += 1;
                } else {
                    self.st.n.data_retired += 1;
                }
            }
            Ok(Retirement::NotHeld) => self.st.n.retire_stale += 1,
            Ok(Retirement::NotYet) => {
                self.st.n.retire_refused += 1;
                return false;
            }
            Err(moq5_core::Error::CLOSED) => {}
            _ => {
                self.st.fail(Fault::Outcome("retirement", 0));
                return false;
            }
        }
        let l = &mut self.st.locals[local];
        l.in_done = true;
        l.owned_retry = false;
        true
    }

    fn stream_input(&mut self, local: usize, wire: &mut Wire, now: Instant) -> bool {
        let mut progress = false;
        let Some(index) = self.st.locals[local].wire else {
            return false;
        };
        let side = self.st.side;
        let bidi = wire.streams[index].bidi;
        let peer_opened = self.st.locals[local].peer_opened;
        let has_in = bidi || peer_opened;
        let has_out = bidi || !peer_opened;

        // The peer's stop of this side's direction.
        if has_out && !self.st.locals[local].peer_stop_done {
            if let Some(code) = wire.streams[index].dir[side].stop {
                if self.peer_stop_entry(local, code, now) {
                    wire.streams[index].dir[side].stop_read = true;
                    progress = true;
                }
            }
        }

        // "All data committed": the peer read every byte and the FIN.
        {
            let out = &wire.streams[index].dir[side];
            let l = &self.st.locals[local];
            if has_out
                && l.role == Role::DataOut
                && !l.committed
                && out.fin
                && out.fin_read
                && out.buf.is_empty()
            {
                if let Some(stream) = l.stream {
                    self.st.locals[local].committed = true;
                    let _ = self.session.on_data_committed(stream, now);
                    progress = true;
                }
            }
        }

        if !has_in || self.st.locals[local].in_done {
            return progress;
        }

        // A signal the session owes a second delivery.
        if let Some(owed) = self.st.locals[local].owed {
            let result = self.session.redeliver(owed, now).map(|_| ());
            if self.settle_signal(local, result) {
                return true;
            }
            return progress;
        }

        // A delivery the session kept: the empty call resumes it.
        if self.st.locals[local].owned_retry {
            let Some(stream) = self.st.locals[local].stream else {
                return progress;
            };
            let resumed = if self.st.locals[local].role == Role::Request {
                self.session.resume_bidi(stream, now)
            } else {
                self.session.resume_stream(stream, now)
            };
            self.st.n.owned_retries += 1;
            match resumed {
                Ok(Fed::Pending) => return progress,
                Ok(Fed::Accepted) => {
                    let l = &mut self.st.locals[local];
                    l.owned_retry = false;
                    if l.owned_fin {
                        l.owned_fin = false;
                        l.in_done = true;
                        self.st.n.peer_fins += 1;
                    }
                    progress = true;
                    if self.st.locals[local].in_done {
                        return progress;
                    }
                }
                Err(_) => {
                    self.st.locals[local].owned_retry = false;
                    self.input_failed(now);
                    return progress;
                }
            }
        }

        // A delivery the session did not admit: offered again once it could
        // take it. Nothing more is read meanwhile.
        if let Some(reason) = self.st.locals[local].refused {
            if reason == NotAdmittedReason::NoEntry
                && self.st.locals[local].role == Role::DataIn
                && !self.session.can_admit_data_stream()
            {
                return progress;
            }
            if self.feed(local, now) {
                progress = true;
            }
            if self.st.locals[local].refused.is_some() || self.st.locals[local].in_done {
                return progress;
            }
        }

        // A new peer stream is first read only while fewer than
        // MAX_REFUSED are pending. Streams found together are read one first
        // read at a time, each counted before the next, and a stream stays
        // pending from its first read until the session admits it, however
        // its type and header are split: the bound holds across passes and
        // wakes. Control is exempt. Before setup on drafts 18 and 22 a
        // prefix of every unidirectional stream past the bound is read for
        // its type, whatever the others hold, and a request stream past it
        // is refused.
        {
            let l = &self.st.locals[local];
            if l.peer_opened
                && !l.reading
                && !matches!(l.role, Role::ControlBidi | Role::ControlIn)
                && self.pending_count() >= MAX_REFUSED
            {
                let (role, known) = (l.role, l.discovery);
                if self.early() && role == Role::Request {
                    self.refuse_early(local, wire);
                    return true;
                }
                if self.early() && role == Role::Unrouted {
                    if !known {
                        self.st.locals[local].discovery = true;
                        self.st.n.discoveries += 1;
                    }
                } else {
                    self.st.n.admissions_deferred += 1;
                    return progress;
                }
            }
        }

        // A data stream whose type was read past the bound keeps that
        // prefix, unfed, until the pending streams have room for it.
        if self.st.locals[local].discovery && self.st.locals[local].role == Role::DataIn {
            if self.pending_count() >= MAX_REFUSED {
                self.st.n.admissions_deferred += 1;
                return progress;
            }
            self.st.locals[local].discovery = false;
        }

        // Read: one hold's worth, or of a stream past the bound no more
        // than the longest type, and the FIN once the bytes are in.
        if !wire.streams[index].dir[1 - side].dropped {
            let cap = if self.st.locals[local].discovery {
                TYPE_PREFIX
            } else {
                HOLD_CAP
            };
            let room = cap.saturating_sub(self.st.locals[local].hold.len());
            let got = wire.read(index, side, room);
            if !got.is_empty() {
                self.st.locals[local].hold.extend_from_slice(&got);
                self.st.locals[local].reading = true;
                progress = true;
            }
            let pipe = &mut wire.streams[index].dir[1 - side];
            if pipe.fin && !pipe.fin_read && pipe.buf.is_empty() {
                pipe.fin_read = true;
                self.st.locals[local].hold_fin = true;
                self.st.locals[local].reading = true;
                progress = true;
            }
        }

        // A peer unidirectional stream is routed before any byte is fed.
        if self.st.locals[local].role == Role::Unrouted
            && (!self.st.locals[local].hold.is_empty() || self.st.locals[local].hold_fin)
        {
            match self.route(local, now) {
                Some(true) if self.st.locals[local].discovery => {
                    // Classified: control and padding need no admission; a
                    // data stream joins the pending streams if they have
                    // room. If they do not, it is refused before setup, and
                    // once established it waits with its prefix.
                    if self.st.locals[local].role == Role::DataIn
                        && self.pending_count() >= MAX_REFUSED
                    {
                        if self.early() {
                            self.refuse_early(local, wire);
                            return true;
                        }
                        self.st.n.admissions_deferred += 1;
                        return true;
                    }
                    self.st.locals[local].discovery = false;
                    progress = true;
                }
                Some(true) => progress = true,
                Some(false) => {
                    // Waiting for more of the type, unless the peer reset the
                    // stream after its last reliable byte: settled below.
                    let pipe = &wire.streams[index].dir[1 - side];
                    if !(pipe.reset.is_some() && !pipe.reset_read && pipe.buf.is_empty()) {
                        return progress;
                    }
                }
                None => return true,
            }
        }

        if self.st.locals[local].role != Role::Unrouted
            && (!self.st.locals[local].hold.is_empty() || self.st.locals[local].hold_fin)
            && self.feed(local, now)
        {
            progress = true;
        }

        // The peer's reset, once nothing of the stream is left to read.
        let (reset, reset_read, drained, dropped) = {
            let pipe = &wire.streams[index].dir[1 - side];
            (
                pipe.reset,
                pipe.reset_read,
                pipe.buf.is_empty(),
                pipe.dropped,
            )
        };
        if let Some(code) = reset {
            if !reset_read
                && !dropped
                && drained
                && !self.st.locals[local].in_done
                && self.peer_reset(local, code, now)
            {
                wire.streams[index].dir[1 - side].reset_read = true;
                progress = true;
            }
        }

        // Retirement: the transport dropped the direction after this side's
        // stop and nothing more will come.
        let pipe = &wire.streams[index].dir[1 - side];
        if pipe.dropped
            && !pipe.retire_read
            && !self.st.locals[local].in_done
            && self.retire(local, bidi, now)
        {
            wire.streams[index].dir[1 - side].retire_read = true;
            progress = true;
        }
        progress
    }

    fn inputs(&mut self, wire: &mut Wire, now: Instant) -> bool {
        let mut progress = false;
        if self.st.control_pending {
            match self.session.process_pending(now) {
                Ok(Fed::Accepted) => {
                    self.st.control_pending = false;
                    progress = true;
                }
                Ok(Fed::Pending) => {}
                Err(_) => {
                    self.st.control_pending = false;
                    self.input_failed(now);
                }
            }
        }
        // New peer streams; at the pending-stream limit no new one is taken,
        // except before setup on drafts 18 and 22: a request stream, to be
        // refused, and every unidirectional stream, to learn its type.
        for (index, peer) in wire.streams.iter().enumerate() {
            if self.st.find_wire(index).is_some() || peer.opener == self.st.side {
                continue;
            }
            let bidi = peer.bidi;
            let control = bidi && self.st.bidi_control && self.st.control_bidi.is_none();
            if !control && self.pending_count() >= MAX_REFUSED && !self.early() {
                continue;
            }
            let role = if control {
                Role::ControlBidi
            } else if bidi {
                Role::Request
            } else {
                Role::Unrouted
            };
            let stream = if control {
                None
            } else {
                let stream = StreamRef::peer(self.st.next_peer);
                self.st.next_peer += 1;
                stream
            };
            self.st
                .locals
                .push(Local::new(Some(index), stream, role, true));
            if control {
                self.st.control_bidi = Some(self.st.locals.len() - 1);
            }
            progress = true;
        }
        // Control first, then every other stream.
        let mut order: Vec<usize> = (0..self.st.locals.len()).collect();
        order.sort_by_key(|&local| {
            !matches!(
                self.st.locals[local].role,
                Role::ControlBidi | Role::ControlIn | Role::ControlOut
            )
        });
        for local in order {
            if self.st.fault.is_some() {
                break;
            }
            if self.stream_input(local, wire, now) {
                progress = true;
            }
        }
        // Datagrams are never retained: taken or dropped.
        let peer = 1 - self.st.side;
        while let Some(dgram) = wire.dgrams[peer].pop_front() {
            if self.session.on_datagram(&dgram, now).is_err() {
                self.st.n.datagrams_dropped += 1;
            }
            progress = true;
        }
        progress
    }

    fn timer(&mut self, now: Instant) -> bool {
        if self.st.ticked {
            return false;
        }
        let Some(due) = self.session.next_deadline() else {
            return false;
        };
        if due > now || self.st.tick_blocked == Some((now, Some(due))) {
            return false;
        }
        self.st.ticked = true;
        self.st.n.ticks += 1;
        match self.session.tick(now) {
            Ok(Tick::TerminalOwed) => {
                self.st.tick_blocked = Some((now, self.session.next_deadline()));
                self.st.n.ticks_blocked += 1;
                false
            }
            Ok(Tick::Done) => {
                self.st.tick_blocked = None;
                self.session.next_deadline() != Some(due)
            }
            Ok(_) => {
                // An outcome this host does not know.
                self.st.fail(Fault::Unsupported);
                false
            }
            Err(_) => false,
        }
    }

    /// One drain pass.
    fn pass(&mut self, wire: &mut Wire, now: Instant) -> bool {
        let mut progress = false;
        if self.st.fault.is_some() && !self.session.is_closed() {
            return false;
        }
        let peer = 1 - self.st.side;
        if let Some(code) = wire.closed[peer] {
            if !self.st.peer_close_told {
                self.st.peer_close_told = true;
                let _ = self.session.on_transport_close(code, now);
                self.st.out.clear();
                self.st.dgrams.clear();
                progress = true;
            }
        }
        if self.st.fault.is_none() && !self.st.peer_close_told && !self.st.wire_closed {
            if self.expire_held(now) {
                progress = true;
            }
            if !wire.hold_input[self.st.side] && self.inputs(wire, now) {
                progress = true;
            }
            if self.timer(now) {
                progress = true;
            }
        }
        if self.st.tagged && !self.st.peer_close_told && self.resets() {
            progress = true;
        }
        if self.pump(wire, now) {
            progress = true;
        }
        if self.st.peer_close_told {
            self.st.out.clear();
            self.st.dgrams.clear();
            self.st.claim = None;
        } else {
            if self.owed_resets(wire) {
                progress = true;
            }
            if self.flush(wire, now) {
                progress = true;
            }
            if self.flush_datagrams(wire, now) {
                progress = true;
            }
            if self.st.tagged && self.claim_write(wire) {
                progress = true;
            }
            if self.answer_stops(wire) {
                progress = true;
            }
        }
        progress
    }

    /// Held output past its deadline, before anything this pass writes.
    fn expire_held(&mut self, now: Instant) -> bool {
        let mut progress = false;
        let due: Vec<usize> = self
            .st
            .out
            .values()
            .flat_map(|queue| queue.iter())
            .filter(|held| {
                write_parts(held.action.detail()).is_some_and(|(parts, _)| {
                    held.sent < held.limit.unwrap_or(parts[0].len() + parts[1].len())
                }) && self.st.expired(held.local, held.deadline, now)
            })
            .filter_map(|held| held.local)
            .collect();
        let mut due = due;
        due.sort_unstable();
        due.dedup();
        for local in due {
            self.expire_stream(local, now);
            progress = true;
        }
        progress
    }

    /// Poll every queued event, summarized.
    pub fn drain(&mut self) -> Vec<Seen> {
        let mut seen = Vec::new();
        for _ in 0..1024 {
            match self.session.poll_event() {
                Ok(Some(event)) => seen.push(Seen::from(&event)),
                Ok(None) | Err(_) => break,
            }
        }
        seen
    }

    pub fn counters(&self) -> &Counters {
        &self.st.n
    }

    pub fn fault(&self) -> Option<Fault> {
        self.st.fault
    }

    /// Output records and datagrams this side still holds.
    pub fn held(&self) -> usize {
        self.st.held_count() + self.st.dgrams.len()
    }

    /// Refs of the local data streams this side opened, in order.
    pub fn local_data_streams(&self) -> Vec<StreamRef> {
        self.st
            .locals
            .iter()
            .filter(|l| l.role == Role::DataOut)
            .filter_map(|l| l.stream)
            .collect()
    }

    /// The transport stream of a local data stream.
    pub fn wire_of_local(&self, stream: StreamRef) -> Option<usize> {
        let local = self.st.find(stream, &[Role::DataOut])?;
        self.st.locals[local].wire
    }

    /// The transport stream of a local data stream or a request stream.
    pub fn wire_of_stream(&self, stream: StreamRef) -> Option<usize> {
        let local = self.st.find(stream, &[Role::DataOut, Role::Request])?;
        self.st.locals[local].wire
    }

    /// Peer streams refused for want of an entry, and the reads this side
    /// holds for peer streams: (refused streams, held reads, held bytes).
    pub fn held_reads(&self) -> (usize, usize, usize) {
        let held = self
            .st
            .locals
            .iter()
            .filter(|l| l.peer_opened && !l.hold.is_empty());
        (
            self.refused_count(),
            held.clone().count(),
            held.map(|l| l.hold.len()).sum(),
        )
    }

    /// Peer streams read but not yet admitted by the session.
    pub fn pending_streams(&self) -> usize {
        self.pending_count()
    }

    /// Refs of the request streams the peer opened, in order.
    pub fn served_requests(&self) -> Vec<StreamRef> {
        self.st
            .locals
            .iter()
            .filter(|l| l.role == Role::Request && l.peer_opened)
            .filter_map(|l| l.stream)
            .collect()
    }

    /// The earliest deadline of held output this side would still expire:
    /// unwritten subgroup bytes, or a held datagram.
    pub fn next_held_deadline(&self) -> Option<Instant> {
        let streams = self.st.out.values().flat_map(|queue| queue.iter());
        let writes = streams.filter_map(|held| {
            let (parts, _) = write_parts(held.action.detail())?;
            let unsent = held.sent < held.limit.unwrap_or(parts[0].len() + parts[1].len());
            let local = held.local?;
            let l = &self.st.locals[local];
            let live = self.st.tagged && l.role == Role::DataOut && !l.drop_output && !l.out_done;
            (unsent && live).then_some(held.deadline).flatten()
        });
        writes
            .chain(self.st.dgrams.iter().filter_map(|d| d.deadline))
            .min()
    }

    /// Refs of the peer data streams this side has not finished with.
    pub fn open_peer_data_streams(&self) -> Vec<StreamRef> {
        self.st
            .locals
            .iter()
            .filter(|l| l.role == Role::DataIn && !l.in_done)
            .filter_map(|l| l.stream)
            .collect()
    }
}

/// What `flush_front` did with a key's oldest record.
enum Front {
    Done,
    Expire(usize),
    Blocked(bool),
    Empty,
    Close(u64),
}

impl State {
    /// The prefix a decided end still delivers.
    fn kept_prefix(&self, local: usize) -> u64 {
        let l = &self.locals[local];
        if let Some((_, reliable)) = l.reset_owed {
            return reliable;
        }
        if let Some(claim) = self.claim.as_ref().filter(|c| c.local == local) {
            return claim.reliable;
        }
        l.prefix
    }

    /// The prefix fact for an offered reset: what this side holds and
    /// wrote. A STOP the session took waives the unsent prefix, whatever
    /// the reset's first cause: nothing held is written after it, and the
    /// reset delivers the bytes the transport took. `None` while this side
    /// has seen a STOP the session has not taken yet.
    fn prefix_fact(&self, offer: &DataResetOffer, local: Option<usize>) -> Option<PrefixFact> {
        let Some(local) = local else {
            return Some(PrefixFact::Unopened);
        };
        if offer.state() == ResetState::Unopened {
            return Some(PrefixFact::Unopened);
        }
        let l = &self.locals[local];
        if l.drop_output {
            return l.peer_stop_done.then_some(PrefixFact::Waived);
        }
        Some(self.bytes_fact(offer, local))
    }

    /// The prefix fact from the bytes the transport took alone.
    fn bytes_fact(&self, offer: &DataResetOffer, local: usize) -> PrefixFact {
        let taken = self.accepted(local);
        if taken >= offer.reliable_size() {
            PrefixFact::Handed
        } else if taken == 0 {
            // Nothing of the stream reached the transport: no stream exists.
            PrefixFact::Unopened
        } else {
            PrefixFact::Held
        }
    }

    /// A counted open whose first byte the transport took, to report once.
    fn pending_open_report(&self) -> Option<(usize, OutputOwner)> {
        self.locals.iter().enumerate().find_map(|(index, l)| {
            l.open_pending
                .filter(|_| l.written > 0)
                .map(|owner| (index, owner))
        })
    }

    /// Record the facts of a FIRST_OF_STREAM tag on the stream it opens.
    fn note_tag(&mut self, local: usize, tag: &ActionTag, header: u64) {
        if tag.first_of_stream() {
            self.locals[local].owner = tag.owner();
            self.locals[local].prefix = header;
        }
        if tag.open_counted() {
            self.locals[local].open_pending = tag.owner();
        }
        if tag.reset_final_on_stop() {
            self.locals[local].reset_final_on_stop = true;
            self.n.final_policies += 1;
        }
    }

    /// A STOP of the local data stream `local` seen while it was still
    /// open. For a stream with RESET_FINAL_ON_STOP the obligation is
    /// recorded once; with `native` (REQUEST_ENDED) the session resets the
    /// request itself and nothing is replaced.
    fn stop_final(&mut self, local: usize, native: bool) {
        let l = &mut self.locals[local];
        if !l.reset_final_on_stop || l.final_cancel {
            return;
        }
        l.final_cancel = true;
        l.final_native = native;
        if native {
            return;
        }
        let State { out, locals, n, .. } = self;
        for held in out.values_mut().flat_map(|queue| queue.iter_mut()) {
            cancel_final(locals, n, held);
        }
    }

    fn hold(
        &mut self,
        key: OrderKey,
        local: Option<usize>,
        action: Action<'_>,
        sent: usize,
    ) -> Result<(), ()> {
        let deadline = action.tag().and_then(ActionTag::deadline);
        let final_for = action.tag().and_then(ActionTag::final_for);
        match action.into_owned() {
            Ok(owned) => {
                *self.live.lock().unwrap_or_else(|e| e.into_inner()) += 1;
                self.n.held_records += 1;
                self.out.entry(key).or_default().push_back(Held {
                    local,
                    action: owned,
                    sent,
                    limit: None,
                    deadline,
                    final_for,
                    cancel: None,
                    live: Arc::clone(&self.live),
                });
                Ok(())
            }
            Err((action, _)) => {
                // Not delivered; the borrow ends here. The caller closes the
                // session with an internal error.
                drop(action);
                Err(())
            }
        }
    }

    /// Take one polled action: offer it directly when nothing older of its
    /// key is held, its fence is open and it has not expired; hold the
    /// rest. Never waits while the action borrows the session.
    fn take(&mut self, wire: &mut Wire, action: Action<'_>, now: Instant) -> After {
        let key = self.key(action.order_key());
        match action.detail() {
            ActionDetail::SetDataPriority { .. } => return After::Nothing, // no scheduler here
            ActionDetail::Unknown { .. } => {
                self.fail(Fault::Unsupported);
                return After::Nothing;
            }
            ActionDetail::CloseSession { code, .. } => return After::Close(*code),
            ActionDetail::SendDatagram { bytes } => {
                let tag = action.tag();
                if self.dgrams.len() >= DGRAM_HOLD || bytes.len() > PIPE_CAP {
                    self.n.datagrams_dropped += 1;
                } else {
                    self.dgrams.push_back(HeldDatagram {
                        bytes: bytes.to_vec(),
                        deadline: tag.and_then(ActionTag::deadline),
                        owner: tag.and_then(ActionTag::owner),
                    });
                }
                return After::Nothing;
            }
            _ => {}
        }
        let Some(local) = self.local_for_output(wire, action.detail()) else {
            match action.detail() {
                ActionDetail::ResetData { .. } => self.n.resets_unknown += 1,
                ActionDetail::StopData { .. } => {} // the stream already ended here
                ActionDetail::OpenBidiStream { .. } | ActionDetail::SendData { .. } => {
                    self.fail(Fault::Streams);
                }
                _ => self.fail(Fault::Order),
            }
            return After::Nothing;
        };
        if let (Some(tag), ActionDetail::SendData { header, .. }) = (action.tag(), action.detail())
        {
            self.note_tag(local, tag, header.as_bytes().len() as u64);
        }
        match action.detail() {
            ActionDetail::ResetData {
                error_code,
                reliable_size,
                ..
            } if self.tagged => {
                // Written once the stream's earlier bytes are, apart from the
                // poll order; a reset already held keeps its own code.
                let reliable = reliable_size.unwrap_or(0);
                self.locals[local].end_known = true;
                if !self.locals[local].out_done && !self.holds_reset(local) {
                    self.trim(local, reliable);
                    self.locals[local].reset_owed = Some((*error_code, reliable));
                }
                return After::Nothing;
            }
            ActionDetail::StopData { .. } if self.locals[local].stop_sent => return After::Nothing,
            ActionDetail::ResetBidiStream { error_code, .. }
            | ActionDetail::AbortBidiStream { error_code, .. } => {
                // A teardown supersedes the stream's held bytes: they are
                // dropped and the reset (and, for an abort, the STOP) go now,
                // not behind them. An ordinary FIN still waits for its bytes.
                let abort = matches!(action.detail(), ActionDetail::AbortBidiStream { .. });
                self.teardown(wire, local, *error_code, abort);
                return After::Nothing;
            }
            _ => {}
        }
        let busy = self.out.get(&key).is_some_and(|queue| !queue.is_empty());
        let final_for = action.tag().and_then(ActionTag::final_for);
        // A final message a STOP replaces is never offered: it is held, and
        // its record resets the request stream in its place.
        let cancelled = final_for.is_some_and(|owner| self.cancel_owed(owner));
        let fenced = cancelled || final_for.is_some_and(|owner| self.final_waits(owner));
        let expired = self.expired(Some(local), action.tag().and_then(ActionTag::deadline), now);
        if fenced && !cancelled {
            self.n.final_waits += 1;
        }
        let mut sent = 0;
        if !busy && !fenced && !expired {
            match self.apply(wire, local, action.detail()) {
                Applied::All => return After::Nothing,
                Applied::Prefix(n) => sent = n,
            }
        }
        if self.hold(key, Some(local), action, sent).is_err() {
            self.fail(Fault::NoMemory);
            return After::Abandon;
        }
        if cancelled {
            let State { out, locals, n, .. } = self;
            if let Some(held) = out.get_mut(&key).and_then(VecDeque::back_mut) {
                cancel_final(locals, n, held);
            }
        }
        if expired {
            After::Expire(local)
        } else {
            After::Nothing
        }
    }

    /// Offer a borrowed action to the transport without waiting.
    fn apply(&mut self, wire: &mut Wire, local: usize, detail: &ActionDetail<'_>) -> Applied {
        if let Some((parts, fin)) = borrowed_parts(detail) {
            let l = &self.locals[local];
            if l.drop_output || l.out_done {
                return Applied::All; // the peer asked this side not to send
            }
            let total = parts[0].len() + parts[1].len();
            let (took, complete) = self.try_write(wire, local, parts, fin, 0, total);
            return if complete {
                Applied::All
            } else {
                Applied::Prefix(took)
            };
        }
        let side = self.side;
        let index = self.locals[local].wire;
        match detail {
            ActionDetail::ResetData {
                error_code,
                reliable_size,
                ..
            } => {
                if !self.locals[local].out_done {
                    if let Some(index) = index {
                        let reliable = reliable_size.unwrap_or(0);
                        if !wire.try_reset(index, side, *error_code, reliable) {
                            self.n.resets_refused += 1;
                            return Applied::Prefix(0);
                        }
                    }
                    self.locals[local].out_done = true;
                }
            }
            ActionDetail::StopData { error_code, .. }
            | ActionDetail::StopBidiStream { error_code, .. } => {
                if let Some(index) = index {
                    wire.stop(index, side, *error_code);
                }
                self.locals[local].stop_sent = true;
            }
            ActionDetail::ResetBidiStream { error_code, .. } => {
                if !self.locals[local].out_done {
                    if let Some(index) = index {
                        if !wire.try_reset(index, side, *error_code, 0) {
                            self.n.resets_refused += 1;
                            return Applied::Prefix(0);
                        }
                    }
                    self.locals[local].out_done = true;
                }
            }
            ActionDetail::AbortBidiStream { error_code, .. } => {
                if let Some(index) = index {
                    if !self.locals[local].out_done && !wire.try_reset(index, side, *error_code, 0)
                    {
                        self.n.resets_refused += 1;
                        return Applied::Prefix(0);
                    }
                    wire.stop(index, side, *error_code);
                }
                self.locals[local].out_done = true;
                self.locals[local].stop_sent = true;
            }
            _ => self.fail(Fault::Unsupported),
        }
        Applied::All
    }

    /// Write the oldest held record of `key`, or say why it waits.
    fn flush_front(&mut self, wire: &mut Wire, key: OrderKey, now: Instant) -> Front {
        let Some(front) = self.out.get(&key).and_then(VecDeque::front) else {
            return Front::Empty;
        };
        if let (Some(code), Some(local)) = (front.cancel, front.local) {
            if !self.locals[local].out_done {
                if let Some(index) = self.locals[local].wire {
                    if !wire.try_reset(index, self.side, code, 0) {
                        self.n.resets_refused += 1;
                        return Front::Blocked(false);
                    }
                }
                self.locals[local].out_done = true;
            }
            if let Some(queue) = self.out.get_mut(&key) {
                queue.pop_front(); // releases the record once
            }
            return Front::Done;
        }
        if let Some(owner) = front.final_for {
            if front.sent == 0 && self.final_waits(owner) {
                self.n.final_waits += 1;
                return Front::Blocked(false);
            }
        }
        let Some(local) = front.local else {
            return Front::Empty;
        };
        if let OwnedActionDetail::CloseSession { code, .. } = front.action.detail() {
            return Front::Close(*code);
        }
        if let Some((parts, fin)) = write_parts(front.action.detail()) {
            let l = &self.locals[local];
            if !(l.drop_output || l.out_done) {
                let total = front.limit.unwrap_or(parts[0].len() + parts[1].len());
                let trimmed = front.limit.is_some();
                if front.sent < total && self.expired(Some(local), front.deadline, now) {
                    return Front::Expire(local);
                }
                let sent = front.sent;
                // The parts borrow the held record; copy the remainder so the
                // record can be updated after the write.
                let parts: [Vec<u8>; 2] = [parts[0].to_vec(), parts[1].to_vec()];
                let (took, complete) = self.try_write(
                    wire,
                    local,
                    [&parts[0], &parts[1]],
                    fin && !trimmed,
                    sent,
                    total,
                );
                if let Some(front) = self.out.get_mut(&key).and_then(VecDeque::front_mut) {
                    front.sent += took;
                }
                if !complete {
                    return Front::Blocked(took > 0);
                }
            }
        } else {
            let detail = match front.action.detail() {
                OwnedActionDetail::ResetData {
                    error_code,
                    reliable_size,
                    stream,
                } => Some(ActionDetail::ResetData {
                    stream: *stream,
                    error_code: *error_code,
                    reliable_size: *reliable_size,
                }),
                OwnedActionDetail::StopData { stream, error_code } => {
                    Some(ActionDetail::StopData {
                        stream: *stream,
                        error_code: *error_code,
                    })
                }
                OwnedActionDetail::ResetBidiStream { stream, error_code } => {
                    Some(ActionDetail::ResetBidiStream {
                        stream: *stream,
                        error_code: *error_code,
                    })
                }
                OwnedActionDetail::StopBidiStream { stream, error_code } => {
                    Some(ActionDetail::StopBidiStream {
                        stream: *stream,
                        error_code: *error_code,
                    })
                }
                OwnedActionDetail::AbortBidiStream { stream, error_code } => {
                    Some(ActionDetail::AbortBidiStream {
                        stream: *stream,
                        error_code: *error_code,
                    })
                }
                _ => None,
            };
            match detail {
                Some(detail) => {
                    if let Applied::Prefix(_) = self.apply(wire, local, &detail) {
                        return Front::Blocked(false); // the transport refused the reset
                    }
                }
                None => self.fail(Fault::Unsupported),
            }
        }
        if let Some(queue) = self.out.get_mut(&key) {
            queue.pop_front(); // releases the record once
        }
        Front::Done
    }
}

/// How much of an offered action the transport took.
enum Applied {
    All,
    Prefix(usize),
}

/// One event, summarized as owned values for a test to assert on.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Seen {
    pub kind: Option<moq5_core::EventKind>,
    pub subscription: Option<moq5_core::Subscription>,
    pub publication: Option<moq5_core::Publication>,
    pub fetch: Option<moq5_core::Fetch>,
    pub announcement: Option<moq5_core::Announcement>,
    pub namespace_subscription: Option<moq5_core::NamespaceSubscription>,
    /// A datagram object, or a chunk that ends its object.
    pub flag: bool,
    pub group: u64,
    pub object: u64,
    pub payload: Vec<u8>,
    pub text: Vec<u8>,
    pub code: u64,
    pub count: u64,
    pub track_status: Option<moq5_core::TrackStatusRequest>,
    pub track_subscription: Option<moq5_core::TrackSubscription>,
    pub fill: Option<moq5_core::Fill>,
    /// The request a request-scoped event names.
    pub request: Option<moq5_core::RequestHandle>,
    pub largest: Option<moq5_core::Location>,
    pub start: Option<moq5_core::Location>,
    pub end: Option<moq5_core::Location>,
    /// An update's Request ID.
    pub id: Option<u64>,
    /// The answer to an update is the application's.
    pub owed: Option<bool>,
    pub forward: Option<bool>,
    pub location: Option<moq5_core::LocationFilter>,
    pub name: Vec<u8>,
    pub cluster: Option<moq5_core::ClusterPath>,
    pub target: Option<moq5_core::RedirectTarget>,
    pub outcome: Option<moq5_core::UpdateOutcome>,
    pub fill_end: Option<moq5_core::FillEnd>,
    /// The object properties an object, its first chunk or a fill or fetch
    /// object carried.
    pub properties: Vec<u8>,
    /// The authorization tokens a setup or a request carried: (type, value).
    pub tokens: Vec<(u64, Vec<u8>)>,
    /// EXPIRES a PUBLISH or its acceptance carried.
    pub expires_ms: Option<u64>,
    pub priority: Option<u8>,
    pub group_order: Option<moq5_core::GroupOrder>,
    /// An outgoing subgroup's terminal: the subgroup, its stream and the
    /// first recognized cause.
    pub subgroup: Option<moq5_core::Subgroup>,
    pub stream: Option<StreamRef>,
    pub terminal_cause: Option<moq5_core::SubgroupTerminalCause>,
}

fn token_list(tokens: &moq5_core::Tokens<'_>) -> Vec<(u64, Vec<u8>)> {
    tokens
        .iter()
        .map(|t| (t.token_type, t.value.to_vec()))
        .collect()
}

fn property_bytes(properties: Option<&moq5_core::Payload<'_>>) -> Vec<u8> {
    properties.map_or_else(Vec::new, |p| p.as_bytes().to_vec())
}

fn joined(ns: &moq5_core::NamespaceView<'_>) -> Vec<u8> {
    ns.parts().flatten().copied().collect()
}

fn namespace_update<H>(seen: &mut Seen, e: &moq5_core::NamespaceUpdateRequested<'_, H>) {
    seen.id = Some(e.update_request_id);
    if let Some(update) = &e.update {
        seen.text = update.prefix.as_ref().map_or_else(Vec::new, joined);
        seen.forward = update.forward;
        seen.cluster = update.cluster.as_ref().map(|c| c.to_path());
    }
}

fn namespace_concluded<H>(seen: &mut Seen, e: &moq5_core::NamespaceUpdateConcluded<'_, H>) {
    seen.id = Some(e.update_request_id);
    seen.outcome = Some(e.outcome);
    seen.code = e.error_code;
    seen.cluster = e.cluster.as_ref().map(|c| c.to_path());
}

fn track_updated<H>(seen: &mut Seen, e: &moq5_core::TrackUpdated<'_, H>) {
    seen.owed = e.answer_owed;
    seen.id = e.update_request_id;
    seen.forward = e.forward;
    seen.count = e.update_count.map_or(0, u64::from);
    if let Some(options) = &e.options {
        seen.location = options.location;
        seen.largest = options.resolution.and_then(|r| r.largest);
    }
}

impl From<&Event<'_>> for Seen {
    fn from(event: &Event<'_>) -> Seen {
        let mut seen = Seen {
            kind: Some(event.kind()),
            ..Seen::default()
        };
        match event {
            Event::SessionClosed(e) => {
                seen.code = e.code;
                seen.text = e.reason.to_vec();
            }
            Event::SubscribeRequest(e) => {
                seen.subscription = Some(e.subscription);
                seen.text = e.track_name.to_vec();
            }
            Event::SubscribeOk(e) => seen.subscription = Some(e.subscription),
            Event::ObjectReceived(e) => {
                seen.subscription = e.subscription;
                seen.publication = e.publication;
                seen.group = e.group_id;
                seen.object = e.object_id;
                seen.payload = e
                    .payload
                    .as_ref()
                    .map_or_else(Vec::new, |p| p.as_bytes().to_vec());
                seen.flag = e.datagram;
                seen.properties = property_bytes(e.properties.as_ref());
            }
            Event::SubgroupFinished(e) | Event::SubgroupReset(e) => {
                seen.subscription = e.subscription;
                seen.publication = e.publication;
                seen.group = e.group_id;
                seen.code = e.error_code.unwrap_or(0);
            }
            Event::SubscribeDone(e) => {
                seen.subscription = Some(e.handle);
                seen.code = e.status_code;
                seen.count = e.stream_count;
                seen.text = e.reason.to_vec();
            }
            Event::PublishRequest(e) => {
                seen.publication = Some(e.publication);
                seen.text = e.track_name.to_vec();
                seen.tokens = token_list(&e.tokens);
                seen.expires_ms = e.expires_ms;
            }
            Event::PublishOk(e) => {
                seen.publication = Some(e.publication);
                seen.expires_ms = e.expires_ms;
                seen.priority = Some(e.subscriber_priority);
                seen.group_order = e.group_order;
                seen.forward = Some(e.send_allowed);
            }
            Event::SetupComplete(e) => seen.tokens = token_list(&e.tokens),
            Event::PublishFinished(e) => {
                seen.publication = Some(e.handle);
                seen.code = e.status_code;
                seen.count = e.stream_count;
                seen.text = e.reason.to_vec();
            }
            Event::FetchRequest(e) => {
                seen.fetch = Some(e.fetch);
                seen.text = e.track_name.to_vec();
                seen.group = e.end_group;
                seen.object = e.end_object;
                seen.start = Some(e.start);
            }
            Event::FetchOk(e) => {
                seen.fetch = Some(e.fetch);
                seen.group = e.end_group;
                seen.object = e.end_object;
            }
            Event::FetchObject(e) => {
                seen.fetch = Some(e.fetch);
                seen.group = e.group_id;
                seen.object = e.object_id;
                seen.payload = e
                    .payload
                    .as_ref()
                    .map_or_else(Vec::new, |p| p.as_bytes().to_vec());
                seen.properties = property_bytes(e.properties.as_ref());
            }
            Event::FetchComplete(e) => seen.fetch = Some(e.handle),
            Event::FetchCancelled(e) => {
                seen.fetch = Some(e.handle);
                seen.code = e.stream_error_code.unwrap_or(0);
            }
            Event::FetchReset(e) => {
                seen.fetch = Some(e.fetch);
                seen.code = e.error_code;
            }
            Event::FetchError(e) => {
                seen.fetch = Some(e.handle);
                seen.code = e.error_code;
            }
            Event::Unsubscribed(e) => seen.subscription = Some(e.handle),
            Event::Goaway(e) => seen.text = e.new_session_uri.to_vec(),
            Event::NamespacePublished(e) => {
                seen.announcement = Some(e.announcement);
                seen.text = e.namespace.parts().flatten().copied().collect();
                seen.cluster = e.cluster.as_ref().map(|c| c.to_path());
            }
            Event::NamespaceAccepted(e) | Event::NamespaceDone(e) => {
                seen.announcement = Some(e.handle);
            }
            Event::NamespaceSubscriptionRequest(e) => {
                seen.namespace_subscription = Some(e.subscription);
                seen.text = e.prefix.parts().flatten().copied().collect();
                seen.tokens = token_list(&e.tokens);
            }
            Event::NamespaceSubscriptionOk(e) => seen.namespace_subscription = Some(e.handle),
            Event::NamespaceFound(e) | Event::NamespaceGone(e) => {
                seen.namespace_subscription = Some(e.subscription);
                seen.text = e.suffix.parts().flatten().copied().collect();
                seen.cluster = e.cluster.as_ref().map(|c| c.to_path());
            }
            Event::ObjectChunk(e) => {
                seen.subscription = e.subscription;
                seen.group = e.group_id;
                seen.object = e.object_id;
                seen.payload = e
                    .chunk
                    .as_ref()
                    .map_or_else(Vec::new, |p| p.as_bytes().to_vec());
                seen.flag = e.end.is_some();
                seen.properties =
                    property_bytes(e.start.as_ref().and_then(|s| s.properties.as_ref()));
            }
            Event::OutgoingSubgroupTerminated(e) => {
                seen.subscription = e.subscription;
                seen.publication = e.publication;
                seen.subgroup = Some(e.subgroup);
                seen.stream = Some(e.stream);
                seen.group = e.group_id;
                seen.code = e.error_code;
                seen.terminal_cause = Some(e.cause);
            }
            Event::SubscribeError(e) => {
                seen.subscription = Some(e.handle);
                seen.code = e.error_code;
                seen.text = e.reason.to_vec();
            }
            Event::PublishUnsubscribed(e) => {
                seen.publication = Some(e.handle);
                seen.code = e.stream_error_code.unwrap_or(0);
            }
            Event::NamespaceRejected(e) | Event::NamespaceCancelled(e) => {
                seen.announcement = Some(e.handle);
                seen.code = e.error_code;
            }
            Event::NamespaceTerminated(e) => {
                seen.announcement = Some(e.handle);
                seen.code = e.stream_error_code;
            }
            Event::NamespaceSubscriptionError(e) => {
                seen.namespace_subscription = Some(e.handle);
                seen.code = e.error_code;
            }
            Event::TrackStatusRequest(e) => {
                seen.track_status = Some(e.request);
                seen.text = e.track_name.to_vec();
            }
            Event::TrackStatusOk(e) => {
                seen.track_status = Some(e.request);
                seen.largest = e.largest;
                seen.payload = e.track_properties.to_vec();
            }
            Event::TrackStatusError(e) => {
                seen.track_status = Some(e.handle);
                seen.code = e.error_code;
            }
            Event::SubscribeTracksRequest(e) => {
                seen.track_subscription = Some(e.request);
                seen.text = joined(&e.prefix);
                seen.forward = Some(e.forward);
            }
            Event::SubscribeTracksOk(e) => seen.track_subscription = Some(e.handle),
            Event::SubscribeTracksError(e) => {
                seen.track_subscription = Some(e.handle);
                seen.code = e.error_code;
            }
            Event::SubscribeTracksCancelled(e) => seen.track_subscription = Some(e.handle),
            Event::PublishBlocked(e) => {
                seen.track_subscription = Some(e.request);
                seen.text = joined(&e.suffix);
                seen.name = e.track_name.to_vec();
            }
            Event::RequestRedirect(e) => {
                seen.request = Some(e.request);
                seen.code = e.error_code;
                seen.text = e.reason.to_vec();
                seen.target = e.target().ok();
            }
            Event::RequestGoaway(e) => {
                seen.request = Some(e.request);
                seen.text = e.new_session_uri.to_vec();
                seen.count = e.timeout_ms;
            }
            Event::SubscribeUpdated(e) => {
                seen.subscription = Some(e.handle);
                track_updated(&mut seen, e);
            }
            Event::PublishUpdated(e) => {
                seen.publication = Some(e.handle);
                track_updated(&mut seen, e);
            }
            Event::SubscriptionUpdateOk(e) => {
                seen.subscription = Some(e.handle);
                seen.largest = e.largest;
            }
            Event::PublicationUpdateOk(e) => {
                seen.publication = Some(e.handle);
                seen.largest = e.largest;
            }
            Event::PublishState(e) => {
                seen.request = Some(e.request);
                seen.forward = e.forward;
                seen.largest = e.largest;
                seen.location = e.location;
            }
            Event::NamespaceUpdateRequest(e) => {
                seen.announcement = Some(e.handle);
                namespace_update(&mut seen, e);
            }
            Event::NamespaceSubscriptionUpdateRequest(e) => {
                seen.namespace_subscription = Some(e.handle);
                namespace_update(&mut seen, e);
            }
            Event::SubscribeTracksUpdateRequest(e) => {
                seen.track_subscription = Some(e.handle);
                namespace_update(&mut seen, e);
            }
            Event::NamespaceUpdateResult(e) => {
                seen.announcement = Some(e.handle);
                namespace_concluded(&mut seen, e);
            }
            Event::NamespaceSubscriptionUpdateResult(e) => {
                seen.namespace_subscription = Some(e.handle);
                namespace_concluded(&mut seen, e);
            }
            Event::SubscribeTracksUpdateResult(e) => {
                seen.track_subscription = Some(e.handle);
                namespace_concluded(&mut seen, e);
            }
            Event::FillCreated(e) => {
                seen.fill = Some(e.fill.fill);
                seen.subscription = e.fill.subscription;
                seen.publication = e.fill.publication;
            }
            Event::FillRequest(e) => {
                seen.fill = Some(e.fill.fill);
                seen.subscription = e.fill.subscription;
                seen.start = Some(e.start);
                seen.end = Some(e.end);
            }
            Event::FillObject(e) => {
                seen.properties = property_bytes(e.properties.as_ref());
                seen.fill = Some(e.fill.fill);
                seen.subscription = e.fill.subscription;
                seen.group = e.group_id;
                seen.object = e.object_id;
                seen.payload = e
                    .payload
                    .as_ref()
                    .map_or_else(Vec::new, |p| p.as_bytes().to_vec());
            }
            Event::FillGap(e) => {
                seen.fill = Some(e.fill.fill);
                seen.end = Some(e.end);
            }
            Event::FillTerminal(e) => {
                seen.fill = Some(e.fill.fill);
                seen.fill_end = Some(e.reason);
            }
            _ => {}
        }
        seen
    }
}

/// Two sides joined by the transport, side 0 the client.
#[derive(Debug)]
pub struct Pair {
    pub wire: Wire,
    pub hosts: [Host; 2],
    pub now: Instant,
    pub version: Version,
    live: Arc<Mutex<i64>>,
}

pub const CLIENT: usize = 0;
pub const SERVER: usize = 1;

impl Pair {
    /// Both sessions created, bound where the profile needs it, and started.
    pub fn new(version: Version, options: &Options) -> moq5_core::Result<Pair> {
        let now = Instant::from_micros(1);
        let live = Arc::new(Mutex::new(0));
        let mut wire = Wire::new(MAX_STREAMS);
        wire.write_limit = options.write_limit;
        wire.read_limit = options.read_limit;
        wire.drop_after_stop = options.drop_after_stop;
        wire.rng = options.seed.map(crate::wire::Rng::new);
        let mut hosts = [
            Host::new(CLIENT, version, options, Arc::clone(&live), now)?,
            Host::new(SERVER, version, options, Arc::clone(&live), now)?,
        ];
        hosts[CLIENT].session.start(now)?;
        if version != Version::DRAFT_16 {
            hosts[SERVER].session.start(now)?;
        }
        Ok(Pair {
            wire,
            hosts,
            now,
            version,
            live,
        })
    }

    fn begin_wake(&mut self) {
        for host in &mut self.hosts {
            host.st.ticked = false;
        }
    }

    fn pass(&mut self) -> bool {
        let now = self.now;
        let [a, b] = &mut self.hosts;
        let x = a.pass(&mut self.wire, now);
        let y = b.pass(&mut self.wire, now);
        x || y
    }

    /// Drive both sides until a pass moves nothing (true) or `max_passes`
    /// ran (false). Each pass is a wake one microsecond after the last.
    pub fn run(&mut self, max_passes: u32) -> bool {
        for _ in 0..max_passes {
            self.now = self.now.checked_add_micros(1).unwrap_or(self.now);
            self.begin_wake();
            if !self.pass() {
                return true;
            }
        }
        false
    }

    /// One wake at `at` (or now, if later): passes until nothing moves.
    pub fn wake(&mut self, at: Instant, max_passes: u32) -> bool {
        if at > self.now {
            self.now = at;
        }
        self.begin_wake();
        for _ in 0..max_passes {
            if !self.pass() {
                return true;
            }
        }
        false
    }

    pub fn session(&mut self, side: usize) -> &mut Session {
        &mut self.hosts[side].session
    }

    pub fn drain(&mut self, side: usize) -> Vec<Seen> {
        self.hosts[side].drain()
    }

    /// Held output records not yet released, across both sides.
    pub fn live_records(&self) -> i64 {
        *self.live.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The counter of held records, to read after the pair is gone.
    pub fn live_counter(&self) -> Arc<Mutex<i64>> {
        Arc::clone(&self.live)
    }

    /// Both sessions established and no fault.
    pub fn established(&self) -> bool {
        self.hosts
            .iter()
            .all(|h| h.session.state() == SessionState::Established && h.fault().is_none())
    }

    /// The side's peer sends STOP_SENDING on the data stream `stream` the
    /// side opened.
    pub fn peer_stops_data(&mut self, side: usize, stream: StreamRef, code: u64) -> bool {
        let Some(index) = self.hosts[side].wire_of_local(stream) else {
            return false;
        };
        self.wire.stop(index, 1 - side, code);
        true
    }

    /// Deliver a STOP of the side's stream `stream` straight to the side's
    /// host, as its transport would report one, bypassing the wire's own
    /// deduplication: the host boundary repeated STOPs reach.
    pub fn deliver_peer_stop(&mut self, side: usize, stream: StreamRef, code: u64) -> bool {
        let host = &mut self.hosts[side];
        let Some(local) = host.st.find(stream, &[Role::DataOut, Role::Request]) else {
            return false;
        };
        let now = self.now;
        host.peer_stop_entry(local, code, now)
    }

    /// The side's peer sends STOP_SENDING on the request stream `stream`.
    pub fn peer_stops_request(&mut self, side: usize, stream: StreamRef, code: u64) -> bool {
        let Some(index) = self.hosts[side].wire_of_stream(stream) else {
            return false;
        };
        self.wire.stop(index, 1 - side, code);
        true
    }

    /// The side's peer writes raw bytes on the request stream `stream`
    /// after whatever its session wrote there.
    pub fn peer_writes_request(&mut self, side: usize, stream: StreamRef, bytes: &[u8]) -> bool {
        let Some(index) = self.hosts[side].wire_of_stream(stream) else {
            return false;
        };
        let pipe = &mut self.wire.streams[index].dir[1 - side];
        if pipe.fin || pipe.reset.is_some() {
            return false;
        }
        pipe.buf.extend(bytes);
        pipe.taken += bytes.len() as u64;
        true
    }

    /// The earliest deadline of the side's held output (see
    /// [`Host::next_held_deadline`]).
    pub fn next_held_deadline(&self, side: usize) -> Option<Instant> {
        self.hosts[side].next_held_deadline()
    }

    /// The side's host polls at most `actions` actions in a pass (0, the
    /// default, polls until the session has none): what the session queued
    /// later is taken in later passes, after the reset transaction ran
    /// again.
    pub fn set_poll_budget(&mut self, side: usize, actions: u32) {
        self.hosts[side].st.poll_budget = actions;
    }

    /// What the side wrote on its data or request stream `stream`, as the
    /// transport took it.
    pub fn trace(&self, side: usize, stream: StreamRef) -> Option<&crate::wire::Pipe> {
        let index = self.hosts[side].wire_of_stream(stream)?;
        Some(&self.wire.streams[index].dir[side])
    }

    /// The transport refuses the bytes and FIN the side writes on its data
    /// stream `stream` (resets are still taken); `None` refuses the next
    /// stream the side opens.
    pub fn refuse_data_stream(&mut self, side: usize, stream: Option<StreamRef>) -> bool {
        match stream {
            None => {
                self.wire.refuse_next_stream[side] = true;
                true
            }
            Some(stream) => match self.hosts[side].wire_of_local(stream) {
                Some(index) => {
                    self.wire.refuse_stream[side].push(index);
                    true
                }
                None => false,
            },
        }
    }

    /// The transport refuses the bytes and FIN the side writes on its
    /// request stream `stream`.
    pub fn refuse_request_stream(&mut self, side: usize, stream: StreamRef) -> bool {
        match self.hosts[side].wire_of_stream(stream) {
            Some(index) => {
                self.wire.refuse_stream[side].push(index);
                true
            }
            None => false,
        }
    }

    /// Lift every stream refusal of the side.
    pub fn lift_data_refusal(&mut self, side: usize) {
        self.wire.refuse_stream[side].clear();
        self.wire.refuse_next_stream[side] = false;
    }

    /// The side's peer opens a bidirectional stream with raw bytes on it:
    /// its transport stream.
    pub fn peer_opens_bidi(&mut self, side: usize, bytes: &[u8]) -> Option<usize> {
        let opener = 1 - side;
        let index = self.wire.open(opener, true)?;
        let pipe = &mut self.wire.streams[index].dir[opener];
        pipe.buf.extend(bytes);
        pipe.taken += bytes.len() as u64;
        Some(index)
    }

    /// The side's peer opens a unidirectional stream with raw bytes and
    /// ends it with a FIN, a reset, or not at all.
    pub fn peer_opens_uni(&mut self, side: usize, bytes: &[u8], end: PeerEnd) -> bool {
        let opener = 1 - side;
        let Some(index) = self.wire.open(opener, false) else {
            return false;
        };
        let pipe = &mut self.wire.streams[index].dir[opener];
        pipe.buf.extend(bytes);
        pipe.taken += bytes.len() as u64;
        match end {
            PeerEnd::None => {}
            PeerEnd::Fin => pipe.fin = true,
            PeerEnd::Reset(code) => {
                let len = bytes.len() as u64;
                self.wire.reset(index, opener, code, len);
            }
        }
        true
    }
}

/// How an injected peer stream ends.
#[derive(Clone, Copy, Debug)]
pub enum PeerEnd {
    None,
    Fin,
    Reset(u64),
}
