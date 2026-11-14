//! The transport-host records: routes, retirement reports, the data reset
//! transaction and data-end outcomes.

use super::action::OutputOwner;
use super::event::SubgroupTerminalCause;
use super::raw;
use super::stream::StreamRef;
use crate::{sys, Error, Result};

/// Where the bytes of a peer-opened unidirectional stream belong
/// (`moq_uni_route_t`), from the bytes at its start.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum UniRoute {
    /// The stream type is incomplete. Keep the bytes and ask again, from
    /// the first byte, when more have arrived. A stream that ends here is
    /// fed to no input: a FIN after part of a type is a truncated type (the
    /// host closes the session with a protocol violation), a FIN or reset
    /// after no byte only ends it.
    NeedMore,
    /// The peer's control channel: feed every byte, the leading ones
    /// included, to [`Session::on_control_bytes`](crate::Session::on_control_bytes).
    /// A second such stream is a protocol violation.
    Control,
    /// An object stream: feed every byte, the leading ones included, to the
    /// data feed.
    Data,
    /// Padding: discard its bytes.
    Padding,
    /// A complete type the profile does not define: the session must be
    /// closed (draft 18 section 3.4, draft 22 section 6.4.1).
    UnknownType,
    /// A route this crate does not know: feed the bytes to no input.
    Other(u32),
}

impl UniRoute {
    pub(super) fn from_raw(raw: sys::moq_uni_route_t) -> Result<UniRoute> {
        Ok(match raw {
            sys::MOQ_UNI_ROUTE_NEED_MORE => UniRoute::NeedMore,
            sys::MOQ_UNI_ROUTE_CONTROL => UniRoute::Control,
            sys::MOQ_UNI_ROUTE_DATA => UniRoute::Data,
            sys::MOQ_UNI_ROUTE_PADDING => UniRoute::Padding,
            sys::MOQ_UNI_ROUTE_UNKNOWN_TYPE => UniRoute::UnknownType,
            // NONE is what a failed call leaves; it is never an answer.
            sys::MOQ_UNI_ROUTE_NONE => return Err(Error::INTERNAL),
            other => UniRoute::Other(other),
        })
    }
}

/// The session's answer to a retirement report
/// ([`Session::on_data_stream_retired`](crate::Session::on_data_stream_retired),
/// [`Session::on_bidi_stream_retired`](crate::Session::on_bidi_stream_retired)).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Retirement {
    /// Released: the session no longer holds the stream.
    Released,
    /// The session has not let go yet (the stream is live, its stop or an
    /// action for it is still owed or unpolled); nothing changed. Poll, then
    /// report again.
    NotYet,
    /// The session holds nothing for the stream: never stopped here, or
    /// already ended or retired.
    NotHeld,
}

/// The state of an offered data reset (`moq_data_reset_state_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResetState {
    /// Claimable: the stream's prefix has left the session.
    Ready,
    /// The one outstanding claim, waiting for its commit.
    Claimed,
    /// A subgroup reset whose stream header never left the session: no
    /// transport stream exists.
    Unopened,
    /// A state this crate does not know.
    Other(u32),
}

/// One offered data reset (`moq_data_reset_offer_t`). Its identity is the
/// stream, owner, code, reliable size and cause; the state is a snapshot.
///
/// A host token: it does not carry the session that offered it, and an
/// equal tuple from another session is indistinguishable. The host claims
/// and commits it on the session that offered it.
#[derive(Clone, Copy, Debug)]
pub struct DataResetOffer {
    raw: sys::moq_data_reset_offer_t,
    stream: StreamRef,
}

impl DataResetOffer {
    pub(super) fn from_raw(raw: sys::moq_data_reset_offer_t) -> Result<DataResetOffer> {
        raw::stamp(
            raw.struct_size,
            sys::MOQ_DATA_RESET_OFFER_V0_SIZE,
            std::mem::size_of::<sys::moq_data_reset_offer_t>(),
        )?;
        let stream = StreamRef::from_record(raw.stream_ref)?;
        Ok(DataResetOffer { raw, stream })
    }

    pub(super) fn raw(&self) -> &sys::moq_data_reset_offer_t {
        &self.raw
    }

    /// The state when offered.
    #[must_use]
    pub fn state(&self) -> ResetState {
        match self.raw.state {
            sys::MOQ_DATA_RESET_READY => ResetState::Ready,
            sys::MOQ_DATA_RESET_CLAIMED => ResetState::Claimed,
            sys::MOQ_DATA_RESET_UNOPENED => ResetState::Unopened,
            other => ResetState::Other(other),
        }
    }

    /// The stream.
    #[must_use]
    pub fn stream(&self) -> StreamRef {
        self.stream
    }

    /// The reset's code: its first cause.
    #[must_use]
    pub fn error_code(&self) -> u64 {
        self.raw.error_code
    }

    /// The stream header's length: the bytes the reset still delivers.
    #[must_use]
    pub fn reliable_size(&self) -> u64 {
        self.raw.reliable_size
    }

    /// The owner, as tagged on the stream's actions.
    #[must_use]
    pub fn owner(&self) -> Option<OutputOwner> {
        OutputOwner::from_raw(self.raw.owner._v)
    }

    /// The subgroup's latched terminal cause; `None` when no terminal event
    /// belongs to the reset.
    #[must_use]
    pub fn cause(&self) -> SubgroupTerminalCause {
        SubgroupTerminalCause::from_raw(self.raw.cause)
    }
}

/// Offers are equal when they are the same reset: stream, owner, code,
/// reliable size and cause. The state is a snapshot, not identity.
impl PartialEq for DataResetOffer {
    fn eq(&self, other: &DataResetOffer) -> bool {
        self.raw.stream_ref._v == other.raw.stream_ref._v
            && self.raw.owner._v == other.raw.owner._v
            && self.raw.error_code == other.raw.error_code
            && self.raw.reliable_size == other.raw.reliable_size
            && self.raw.cause == other.raw.cause
    }
}

impl Eq for DataResetOffer {}

/// The outcome of [`Session::offer_data_reset`](crate::Session::offer_data_reset).
///
/// Closed: the three outcomes of an offer poll, as for actions.
#[derive(Clone, Copy, Debug)]
pub enum ResetOfferPoll {
    /// The next reset the host may take, or the claim it already holds.
    Offer(DataResetOffer),
    /// No reset is offered now.
    Empty,
    /// The selected reset waits only for event capacity: poll events, then
    /// offer again.
    Waiting,
}

/// The host's fact about a stream's reliable prefix (`moq_data_prefix_state_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PrefixFact {
    /// The transport accepted the first `reliable_size` bytes.
    Handed,
    /// The host holds unsent prefix bytes and writes them before the reset.
    Held,
    /// The peer stopped the stream and the session took that STOP; the
    /// unsent prefix is not sent. Allowed for a READY reset of a stream
    /// whose STOP the session knows: one the STOP itself caused, or any
    /// other reset of the stream once
    /// [`on_data_stop`](crate::Session::on_data_stop) returned `Ok`. A reset
    /// with an earlier cause keeps its identity, code, cause and terminal
    /// event; WAIVED only says the unsent prefix is not sent. A STOP the
    /// session did not take authorizes nothing.
    Waived,
    /// No transport stream exists, and none will be opened (subgroups only).
    Unopened,
}

impl PrefixFact {
    pub(super) fn raw(self) -> sys::moq_data_prefix_state_t {
        match self {
            PrefixFact::Handed => sys::MOQ_DATA_PREFIX_HANDED,
            PrefixFact::Held => sys::MOQ_DATA_PREFIX_HELD,
            PrefixFact::Waived => sys::MOQ_DATA_PREFIX_WAIVED,
            PrefixFact::Unopened => sys::MOQ_DATA_PREFIX_UNOPENED,
        }
    }
}

/// What a claim did (`moq_data_reset_claim_t`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataResetClaim {
    flags: u32,
    released_data_actions: u32,
}

impl DataResetClaim {
    pub(super) fn from_raw(raw: &sys::moq_data_reset_claim_t) -> Result<DataResetClaim> {
        raw::stamp(
            raw.struct_size,
            sys::MOQ_DATA_RESET_CLAIM_V0_SIZE,
            std::mem::size_of::<sys::moq_data_reset_claim_t>(),
        )?;
        Ok(DataResetClaim {
            flags: raw.flags,
            released_data_actions: raw.released_data_actions,
        })
    }

    /// The claim committed the subgroup's terminal event.
    #[must_use]
    pub fn event_committed(&self) -> bool {
        self.flags & sys::MOQ_DATA_RESET_CLAIM_EVENT_COMMITTED != 0
    }

    /// The reset is settled: nothing is owed at the transport and no commit
    /// follows (an unopened claim).
    #[must_use]
    pub fn settled(&self) -> bool {
        self.flags & sys::MOQ_DATA_RESET_CLAIM_SETTLED != 0
    }

    /// The stream's pending open was resolved as never opened.
    #[must_use]
    pub fn open_suppressed(&self) -> bool {
        self.flags & sys::MOQ_DATA_RESET_CLAIM_OPEN_SUPPRESSED != 0
    }

    /// Queued data actions of the stream whose custody ended in the claim:
    /// the host will never be handed them.
    #[must_use]
    pub fn released_data_actions(&self) -> u32 {
        self.released_data_actions
    }

    /// Flag bits this crate does not know. A host treats any as a fault.
    #[must_use]
    pub fn unknown_flags(&self) -> u32 {
        self.flags
            & !(sys::MOQ_DATA_RESET_CLAIM_EVENT_COMMITTED
                | sys::MOQ_DATA_RESET_CLAIM_SETTLED
                | sys::MOQ_DATA_RESET_CLAIM_OPEN_SUPPRESSED)
    }
}

/// What the session does after a peer STOP on, or the expiry of held data
/// of, a local data stream (`moq_data_end_result_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataEndOutcome {
    /// The session owns the stream's end: exactly one reset for it comes,
    /// by the reset offer or a poll. Stop writing it and drop held bytes past
    /// its prefix.
    ResetFollows,
    /// No native output for the stream follows. Drop what is held of it. A
    /// reset already held, owed or accepted keeps its own code; otherwise,
    /// unless every byte was acknowledged, reset the stream with
    /// `host_reset_code`.
    NoReset {
        /// The code for the host's own reset.
        host_reset_code: u64,
    },
    /// An outcome this crate does not know.
    Other(u32),
}

/// The full answer of a STOP or expiry report.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataEnd {
    /// What follows.
    pub outcome: DataEndOutcome,
    /// The stream's queued data actions the call removed.
    pub released_data_actions: u32,
    /// A FETCH teardown: its request stream is reset too.
    pub request_ended: bool,
    /// Flag bits this crate does not know.
    pub unknown_flags: u32,
}

impl DataEnd {
    pub(super) fn from_raw(raw: &sys::moq_data_end_result_t) -> Result<DataEnd> {
        raw::stamp(
            raw.struct_size,
            sys::MOQ_DATA_END_RESULT_V0_SIZE,
            std::mem::size_of::<sys::moq_data_end_result_t>(),
        )?;
        Ok(DataEnd {
            outcome: match raw.outcome {
                sys::MOQ_DATA_END_RESET_FOLLOWS => DataEndOutcome::ResetFollows,
                sys::MOQ_DATA_END_NO_RESET => DataEndOutcome::NoReset {
                    host_reset_code: raw.host_reset_code,
                },
                other => DataEndOutcome::Other(other),
            },
            released_data_actions: raw.released_data_actions,
            request_ended: raw.flags & sys::MOQ_DATA_END_REQUEST_ENDED != 0,
            unknown_flags: raw.flags & !sys::MOQ_DATA_END_REQUEST_ENDED,
        })
    }
}

/// The host's report of a counted data stream's open (`moq_data_open_outcome_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataOpenOutcome {
    /// The transport accepted the stream's open (for a transport that opens
    /// a stream on its first write, that write's acceptance).
    Opened,
    /// The host cannot tell whether the open was accepted; the owner's
    /// Stream Count becomes the profile's unknown sentinel. A host that
    /// knows the stream was never opened claims its reset as unopened
    /// instead.
    Unknown,
}

impl DataOpenOutcome {
    pub(super) fn raw(self) -> sys::moq_data_open_outcome_t {
        match self {
            DataOpenOutcome::Opened => sys::MOQ_DATA_OPEN_OPENED,
            DataOpenOutcome::Unknown => sys::MOQ_DATA_OPEN_UNKNOWN,
        }
    }
}

/// The outcome of a [`Session::tick`](crate::Session::tick). Time advanced
/// either way.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[must_use]
pub enum Tick {
    /// Time advanced and everything due was done.
    Done,
    /// An outgoing subgroup's terminal is still owed for lack of queue
    /// room. Poll events and actions; the session retries at its own retry
    /// deadline (part of [`Session::next_deadline`](crate::Session::next_deadline))
    /// and at later calls. Not a refusal: the tick already did everything
    /// else that was due.
    TerminalOwed,
}
