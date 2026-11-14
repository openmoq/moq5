//! The sans-I/O session: bytes and time in, actions and events out.
//!
//! A [`Session`] is the native MoQ state machine. A host that owns its
//! transport drives it: it binds the transport, feeds what the transport
//! delivers, polls actions and writes them to the transport, polls events
//! for the application, and supplies time. The session never blocks, reads
//! no clock and opens no socket.
//!
//! # The host contract
//!
//! **Before start.** Draft 22 requires a [`TransportBinding`] before
//! [`Session::start`]; drafts 16 and 18 accept one. A refused bind returns
//! the binding whole. A host that drives the transport itself enables the
//! tagged mode ([`Session::enable_tagged_output`]) on the pristine session,
//! before start. Draft 16 starts on the client only; drafts 18 and 22 start
//! on both sides.
//!
//! **Streams.** Draft 16 carries control on one bidirectional stream
//! ([`ActionDetail::SendControl`], [`Session::on_control_bytes`]). Drafts
//! 18 and 22 carry it on a unidirectional stream each side opens
//! ([`ActionDetail::OpenUniControl`]), and every later
//! [`ActionDetail::SendControl`] continues that stream; the host routes every peer
//! unidirectional stream by its leading bytes with
//! [`Session::route_peer_uni`] and never parses a stream type itself. A
//! peer's reset and a peer's stop are different inputs. The end of a
//! control stream by the peer is reported with
//! [`Session::on_control_stream_ended`]; a transport that retires a stopped
//! stream without a peer terminal reports it with
//! [`Session::on_data_stream_retired`] or
//! [`Session::on_bidi_stream_retired`].
//!
//! **Input custody.** Every feed answers who holds the input:
//! [`Fed::Accepted`] (consumed), [`Fed::Pending`] (retained: drain, then
//! resume with the empty call; never deliver the same bytes again),
//! [`FeedError::NotAdmitted`] (nothing retained: the host keeps the exact
//! input and `fin`, an owned [`Buffer`](crate::Buffer) comes back intact, and delivers it
//! again later) or [`FeedError::Failed`]. A refused peer signal comes back
//! as an [`OwedSignal`] carrying its original arguments. A host bounds
//! refused input: one read per refused stream, a maximum number of refused
//! streams (at that limit it accepts no new peer stream, and the transport's
//! stream limit holds the peer), and control is never counted against it
//! and is served first in every pass.
//!
//! **Output.** [`Session::poll_action`] has three outcomes: an action,
//! empty, or waiting (for event capacity, or in the tagged mode for the
//! reset transaction). A polled [`Action`] borrows the session; the host
//! offers it to the transport without waiting, and holds whatever the
//! transport did not take with [`Action::into_owned`], which is atomic.
//! Actions with the same [`OrderKey`] reach the transport in poll order: a
//! later action never passes a held one with its key. A partial write takes
//! a prefix; the rest and the FIN are still owed, and the FIN goes only
//! after the last byte. In the tagged mode streams may overtake each other,
//! because an owner's final message ([`ActionTag::final_for`]) is handed to
//! the transport only after every data stream of that owner has its FIN or
//! reset accepted, no reset claim of it is outstanding and no datagram of it
//! is held; without tags the host writes all stream output in poll order.
//! A FETCH response stream tagged [`ActionTag::reset_final_on_stop`] keeps
//! that policy until its FIN or reset is accepted: a STOP of it seen before
//! then replaces the FETCH's final message, held or polled later, with a
//! reset of the request stream (`MOQ_STREAM_ERROR_CANCELLED`), unless the
//! session resets the request itself.
//! The host's hold is bounded: when it is full it stops polling, and the
//! session's own queue limits push back. If `into_owned` fails the host
//! drops the action and closes the session with an internal error.
//!
//! **Progress.** Each pass: deliver new input and retry what is owed
//! (`process_pending`, `resume_stream`, owed signals, refused input once
//! [`Session::can_admit_data_stream`] is true); tick when the deadline has
//! come; drain actions until the poll is empty or waits, the transport
//! refuses one, or the hold is full; drain events (the application's answers
//! are advancing calls and may queue more actions); offer held output
//! again. Repeat while a pass made progress (an action, byte or reset
//! reached the transport, an input or event was taken, an owed item
//! completed). Otherwise wait for network input, a writable transport, the
//! deadline ([`Session::next_deadline`]) or an application call. A pass with
//! no progress never spins.
//!
//! # Threads and borrows
//!
//! The owner is `Send` and not `Sync`: a session may move to another thread
//! between calls (the native session has no thread affinity), and only one
//! thread calls into it at a time. Every polled record borrows it
//! exclusively, and so does every field taken out of a record: a field
//! keeps the session borrow after its record is dropped, so nothing a
//! record lends can outlive the session borrow or overlap an advancing
//! call. Records themselves, a [`Payload`] (a native reference) and the
//! views over native arrays ([`NamespaceView`], [`Tokens`]) are neither
//! `Send` nor `Sync`: they stay on the session's current thread, and while
//! one lives the owner cannot move. The plain slices a
//! record lends (its `&[u8]` fields, [`ResolvedToken`]'s value,
//! [`ClusterView`]'s Hop IDs) are ordinary shared slices: a scoped thread
//! may read one while the session borrow it carries lasts, which keeps the
//! session from advancing meanwhile. A payload leaves the session only as a
//! [`Buffer`](crate::Buffer) copy ([`Payload::into_buffer`]); an
//! [`OwnedAction`] holds independent copies. An observation that borrows
//! session storage ([`Session::namespace_owed_update`],
//! [`Session::namespace_cluster`]) holds the session shared, which excludes
//! every advancing call while it lives.
//!
//! # Handles and host tokens
//!
//! Request handles ([`Subscription`], [`Fetch`] and the others) carry the
//! session that issued them; any other session refuses one with
//! [`Error::WRONG_SESSION`](crate::Error::WRONG_SESSION) before C sees it.
//! The host's transport tokens do not: a [`StreamRef`], an [`OutputOwner`]
//! and a [`DataResetOffer`] are scalars the host keeps with the session
//! that issued them. Two sessions can produce equal values, and a session
//! handed another's token cannot tell; the host scopes them.
//!
//! # Requests
//!
//! Every request family the native session offers has its calls here, each
//! building its configuration with the sized initializer and refusing a
//! handle of another session with [`Error::WRONG_SESSION`](crate::Error::WRONG_SESSION)
//! before C sees it. A field a draft cannot carry is refused by the session
//! (typically [`Error::UNSUPPORTED`](crate::Error::UNSUPPORTED) or
//! [`Error::INVAL`](crate::Error::INVAL)), never dropped; so is a value the
//! native configuration has no field for, such as a nonempty redirect target
//! on a PUBLISH or SUBSCRIBE_TRACKS refusal, which the wire could carry but
//! the native API cannot yet express.

mod action;
mod binding;
mod config;
mod custody;
mod event;
mod family;
mod handle;
mod host;
mod owner;
mod payload;
mod query;
mod raw;
mod request;
mod stream;
mod time;
mod view;

pub use action::{
    Action, ActionDetail, ActionKind, ActionPoll, ActionTag, DataPriority, OrderKey, OutputOwner,
    OwnedAction, OwnedActionDetail, StreamHeader,
};
pub use binding::{
    BindRefusal, BindRefused, ConnectionTarget, Fact, FactSet, Facts, TransportBinding, UriPolicy,
    UriTarget, UriVerdict,
};
pub use config::{Perspective, SessionConfig, SessionState};
pub use custody::{
    Fed, FeedError, NotAdmitted, NotAdmittedReason, OwedSignal, PeerSignal, SignalError, WriteError,
};
pub use event::{
    AdmissionOptions, ChunkStart, Event, EventKind, FetchGap, FetchObject, FetchOk, FetchRange,
    FetchRequest, FetchRequestOptions, FillCreated, FillEnd, FillError, FillGap, FillObject,
    FillParent, FillRequest, FillTerminal, Goaway, HandleEvent, Joining, NamespaceInterest,
    NamespacePublished, NamespaceSubscriptionRequest, NamespaceSuffix, NamespaceTerminalCause,
    NamespaceUpdate, NamespaceUpdateConcluded, NamespaceUpdateRequested, ObjectChunk,
    ObjectReceived, ObjectTerminal, OutgoingSubgroupTerminated, PublishBlocked, PublishOk,
    PublishRequest, PublishState, RequestEnded, RequestGoaway, RequestHandle, RequestReady,
    RequestRedirect, RequestRefused, SessionClosed, SetupComplete, StreamTerminated, SubgroupEnded,
    SubgroupTerminalCause, SubscribeFilter, SubscribeOk, SubscribeRequest, SubscribeTracksOptions,
    SubscribeTracksRequest, TrackDone, TrackStatusCause, TrackStatusOk, TrackStatusRequestEvent,
    TrackUpdated, UnknownEvent, UpdateAccepted, UpdateOptions, UpdateOutcome, UpdateResolution,
    UpdatedFilter,
};
pub use family::{
    AcceptTrackStatus, FetchUpdate, NamespaceUpdateConfig, PublishNamespaceConfig,
    PublishStateConfig, SubscribeTracksConfig, TrackStatusConfig,
};
pub use handle::{
    Announcement, Fetch, Fill, NamespaceSubscription, Publication, Subgroup, Subscription,
    TrackStatusRequest, TrackSubscription,
};
pub use host::{
    DataEnd, DataEndOutcome, DataOpenOutcome, DataResetClaim, DataResetOffer, PrefixFact,
    ResetOfferPoll, ResetState, Retirement, Tick, UniRoute,
};
pub use owner::Session;
pub use payload::Payload;
pub use query::{ClusterInfo, FilterWindow, OwedUpdate, PublishInitialParameters, WindowEnd};
pub use request::{
    AcceptFetch, AcceptPublish, AcceptSubscribe, AuthToken, ClusterPath, FetchConfig,
    FetchObjectWrite, FillConfig, JoiningStart, ObjectWrite, PublishConfig, RedirectTarget,
    Rejection, SubgroupConfig, SubscribeConfig, SubscribeNamespaceConfig, SubscriptionUpdate,
    TrackDoneConfig,
};
pub use stream::StreamRef;
pub use time::Instant;
pub use view::{ClusterView, NamespaceView, RequestRole, ResolvedToken, TerminalCause, Tokens};
