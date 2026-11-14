//! Actions: what the session asks the transport to do.

use std::fmt;
use std::marker::PhantomData;

use super::payload::Payload;
use super::raw::{self, field_end};
use super::stream::StreamRef;
use super::time::Instant;
use crate::{sys, Buffer, Error, Result};

/// The outcome of [`Session::poll_action`](crate::Session::poll_action).
///
/// The three outcomes a poll has; closed by the native contract.
#[derive(Debug)]
pub enum ActionPoll<'session> {
    /// The next action. It borrows the session until it is dropped or
    /// converted with [`Action::into_owned`].
    Ready(Action<'session>),
    /// No action is queued.
    Empty,
    /// An action is queued but cannot be handed over yet: its terminal waits
    /// for event capacity (poll events, then poll actions again), or, in the
    /// tagged mode, it is a data reset only the reset transaction takes
    /// ([`Session::offer_data_reset`](crate::Session::offer_data_reset) tells
    /// which). Not empty: more output is coming.
    Waiting,
}

/// An action kind (`moq_action_kind_t`, an open set). Every native kind has
/// a variant; a kind this crate does not know is kept as `Other`.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ActionKind {
    /// `MOQ_ACTION_SEND_CONTROL`.
    SendControl,
    /// `MOQ_ACTION_CLOSE_SESSION`.
    CloseSession,
    /// `MOQ_ACTION_SEND_DATA`.
    SendData,
    /// `MOQ_ACTION_RESET_DATA`.
    ResetData,
    /// `MOQ_ACTION_STOP_DATA`.
    StopData,
    /// `MOQ_ACTION_OPEN_BIDI_STREAM`.
    OpenBidiStream,
    /// `MOQ_ACTION_SEND_BIDI_STREAM`.
    SendBidiStream,
    /// `MOQ_ACTION_CLOSE_BIDI_STREAM`.
    CloseBidiStream,
    /// `MOQ_ACTION_SEND_DATAGRAM`.
    SendDatagram,
    /// `MOQ_ACTION_OPEN_UNI_CONTROL`.
    OpenUniControl,
    /// `MOQ_ACTION_SEND_UNI_CONTROL`.
    SendUniControl,
    /// `MOQ_ACTION_RESET_BIDI_STREAM`.
    ResetBidiStream,
    /// `MOQ_ACTION_STOP_BIDI_STREAM`.
    StopBidiStream,
    /// `MOQ_ACTION_ABORT_BIDI_STREAM`.
    AbortBidiStream,
    /// `MOQ_ACTION_SET_DATA_PRIORITY`.
    SetDataPriority,
    /// A kind this crate does not know.
    Other(u32),
}

impl ActionKind {
    /// The kind of a native code.
    #[must_use]
    pub fn from_code(code: u32) -> ActionKind {
        match code {
            sys::MOQ_ACTION_SEND_CONTROL => ActionKind::SendControl,
            sys::MOQ_ACTION_CLOSE_SESSION => ActionKind::CloseSession,
            sys::MOQ_ACTION_SEND_DATA => ActionKind::SendData,
            sys::MOQ_ACTION_RESET_DATA => ActionKind::ResetData,
            sys::MOQ_ACTION_STOP_DATA => ActionKind::StopData,
            sys::MOQ_ACTION_OPEN_BIDI_STREAM => ActionKind::OpenBidiStream,
            sys::MOQ_ACTION_SEND_BIDI_STREAM => ActionKind::SendBidiStream,
            sys::MOQ_ACTION_CLOSE_BIDI_STREAM => ActionKind::CloseBidiStream,
            sys::MOQ_ACTION_SEND_DATAGRAM => ActionKind::SendDatagram,
            sys::MOQ_ACTION_OPEN_UNI_CONTROL => ActionKind::OpenUniControl,
            sys::MOQ_ACTION_SEND_UNI_CONTROL => ActionKind::SendUniControl,
            sys::MOQ_ACTION_RESET_BIDI_STREAM => ActionKind::ResetBidiStream,
            sys::MOQ_ACTION_STOP_BIDI_STREAM => ActionKind::StopBidiStream,
            sys::MOQ_ACTION_ABORT_BIDI_STREAM => ActionKind::AbortBidiStream,
            sys::MOQ_ACTION_SET_DATA_PRIORITY => ActionKind::SetDataPriority,
            other => ActionKind::Other(other),
        }
    }

    /// The native code.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            ActionKind::SendControl => sys::MOQ_ACTION_SEND_CONTROL,
            ActionKind::CloseSession => sys::MOQ_ACTION_CLOSE_SESSION,
            ActionKind::SendData => sys::MOQ_ACTION_SEND_DATA,
            ActionKind::ResetData => sys::MOQ_ACTION_RESET_DATA,
            ActionKind::StopData => sys::MOQ_ACTION_STOP_DATA,
            ActionKind::OpenBidiStream => sys::MOQ_ACTION_OPEN_BIDI_STREAM,
            ActionKind::SendBidiStream => sys::MOQ_ACTION_SEND_BIDI_STREAM,
            ActionKind::CloseBidiStream => sys::MOQ_ACTION_CLOSE_BIDI_STREAM,
            ActionKind::SendDatagram => sys::MOQ_ACTION_SEND_DATAGRAM,
            ActionKind::OpenUniControl => sys::MOQ_ACTION_OPEN_UNI_CONTROL,
            ActionKind::SendUniControl => sys::MOQ_ACTION_SEND_UNI_CONTROL,
            ActionKind::ResetBidiStream => sys::MOQ_ACTION_RESET_BIDI_STREAM,
            ActionKind::StopBidiStream => sys::MOQ_ACTION_STOP_BIDI_STREAM,
            ActionKind::AbortBidiStream => sys::MOQ_ACTION_ABORT_BIDI_STREAM,
            ActionKind::SetDataPriority => sys::MOQ_ACTION_SET_DATA_PRIORITY,
            ActionKind::Other(code) => code,
        }
    }
}

/// A request's output-owner token (`moq_output_owner_t`). Equal tokens name
/// the same request for as long as any output tagged with it is held, its
/// final message included. It is compared whole and never decoded; it is
/// not a request handle. It is a host token of one session: another session
/// may produce an equal value for another request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OutputOwner(u64);

impl OutputOwner {
    /// The token of a native value; `None` for zero, which names no request.
    #[must_use]
    pub const fn from_raw(value: u64) -> Option<OutputOwner> {
        if value == 0 {
            None
        } else {
            Some(OutputOwner(value))
        }
    }

    /// The native value.
    #[must_use]
    pub const fn as_raw(self) -> u64 {
        self.0
    }

    pub(super) fn raw(self) -> sys::moq_output_owner_t {
        sys::moq_output_owner_t { _v: self.0 }
    }
}

/// The ownership facts the tagged mode hands over with each action
/// (`moq_action_tag_t`). Scalars only.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ActionTag {
    flags: u32,
    owner: Option<OutputOwner>,
    final_for: Option<OutputOwner>,
    deadline: Option<Instant>,
}

impl ActionTag {
    pub(super) fn from_raw(raw: &sys::moq_action_tag_t) -> Result<ActionTag> {
        raw::stamp(
            raw.struct_size,
            sys::MOQ_ACTION_TAG_V0_SIZE,
            std::mem::size_of::<sys::moq_action_tag_t>(),
        )?;
        Ok(ActionTag {
            flags: raw.flags,
            owner: OutputOwner::from_raw(raw.owner._v),
            final_for: OutputOwner::from_raw(raw.final_for._v),
            deadline: (raw.deadline_us != u64::MAX).then(|| Instant::from_micros(raw.deadline_us)),
        })
    }

    /// The request the action belongs to; `None` for session control and a
    /// request message with no data stream.
    #[must_use]
    pub fn owner(&self) -> Option<OutputOwner> {
        self.owner
    }

    /// `Some(owner)` when this action is that owner's final message. The
    /// host hands it to the transport only after every data stream of that
    /// owner (tagged [`first_of_stream`](ActionTag::first_of_stream)) has its
    /// FIN or reset accepted, no reset claim of the owner is outstanding, and
    /// the host holds no datagram of the owner.
    #[must_use]
    pub fn final_for(&self) -> Option<OutputOwner> {
        self.final_for
    }

    /// The delivery deadline of a subgroup object or datagram; `None` for
    /// every FETCH, fill and control action.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// The action opens a local data stream: its header is the stream's
    /// reliable prefix, the bytes every reset of the stream still delivers.
    #[must_use]
    pub fn first_of_stream(&self) -> bool {
        self.flags & sys::MOQ_ACTION_TAG_FIRST_OF_STREAM != 0
    }

    /// With [`first_of_stream`](ActionTag::first_of_stream): the open counts
    /// toward the owner's Stream Count and stays pending until the host
    /// reports it ([`Session::report_data_open`](crate::Session::report_data_open)).
    #[must_use]
    pub fn open_counted(&self) -> bool {
        self.flags & sys::MOQ_ACTION_TAG_OPEN_COUNTED != 0
    }

    /// With [`first_of_stream`](ActionTag::first_of_stream) and an
    /// [`owner`](ActionTag::owner): a FETCH response stream whose FETCH has
    /// a request stream (drafts 18 and 22). The host keeps this policy with
    /// the stream until its FIN or reset is accepted. A STOP it sees on the
    /// stream before then ends the request too: the owner's final message
    /// ([`final_for`](ActionTag::final_for)) is never handed to the
    /// transport, held or polled later, and the request stream's send half
    /// is reset with `MOQ_STREAM_ERROR_CANCELLED` in its place, unless the
    /// session ends the request with a reset of its own
    /// ([`DataEnd::request_ended`](crate::DataEnd::request_ended), or a
    /// reset it queued earlier), which stands alone.
    #[must_use]
    pub fn reset_final_on_stop(&self) -> bool {
        self.flags & sys::MOQ_ACTION_TAG_RESET_FINAL_ON_STOP != 0
    }

    /// Flag bits this crate does not know. A host treats any as a fault.
    #[must_use]
    pub fn unknown_flags(&self) -> u32 {
        self.flags
            & !(sys::MOQ_ACTION_TAG_FIRST_OF_STREAM
                | sys::MOQ_ACTION_TAG_OPEN_COUNTED
                | sys::MOQ_ACTION_TAG_RESET_FINAL_ON_STOP)
    }
}

/// A data stream's scheduling key (`moq_data_priority_t`, MOQT 7.2).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DataPriority {
    /// The subscriber priority; lower goes first.
    pub subscriber_priority: u8,
    /// The publisher priority; lower goes first.
    pub publisher_priority: u8,
    /// Groups in descending order within the owner.
    pub descending: bool,
    /// The stream's owner, an opaque identity.
    pub owner: u64,
    /// The group.
    pub group_id: u64,
    /// The subgroup.
    pub subgroup_id: u64,
}

/// A data stream header, copied inline from the action (at most 32 bytes).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct StreamHeader {
    bytes: [u8; 32],
    len: u8,
}

impl StreamHeader {
    /// The header bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

impl fmt::Debug for StreamHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamHeader")
            .field("len", &self.len)
            .finish()
    }
}

/// Where an action sits in the host's output order: actions with the same
/// key reach the transport in the order they were polled.
///
/// Closed: the keys this crate derives from an action, not a native value.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum OrderKey {
    /// The control channel: the one bidirectional control stream (draft
    /// 16) or this side's unidirectional control stream (drafts 18 and 22),
    /// whichever action writes it.
    Control,
    /// Datagrams.
    Datagrams,
    /// The session's own close.
    Session,
    /// One transport stream.
    Stream(StreamRef),
}

/// What one polled action asks of the transport, its borrowed bytes valid
/// until the action ends. Fields after a kind's frozen prefix are `None`
/// when the library did not report them.
#[non_exhaustive]
#[derive(Debug)]
pub enum ActionDetail<'session> {
    /// Write `bytes` on the control channel: the one bidirectional control
    /// stream on draft 16; on drafts 18 and 22 the unidirectional stream
    /// [`OpenUniControl`](ActionDetail::OpenUniControl) opened, which every
    /// later control message continues.
    SendControl {
        /// The control bytes.
        bytes: &'session [u8],
    },
    /// Close the transport with `code` and `reason`.
    CloseSession {
        /// The session termination code.
        code: u64,
        /// The reason phrase.
        reason: &'session [u8],
    },
    /// Write on a local data stream; its first such action opens it.
    SendData {
        /// The stream.
        stream: StreamRef,
        /// Header bytes written before the payload.
        header: StreamHeader,
        /// The payload, if any.
        payload: Option<Payload<'session>>,
        /// End the stream after these bytes.
        fin: bool,
        /// `0x10000 | subscriber_priority << 8 | publisher_priority`; `None`
        /// when absent.
        scheduling_priority: Option<u64>,
    },
    /// Reset a local data stream.
    ResetData {
        /// The stream.
        stream: StreamRef,
        /// The reset code.
        error_code: u64,
        /// Leading bytes the reset must still deliver (the stream header);
        /// zero for a plain reset at once.
        reliable_size: Option<u64>,
    },
    /// Ask the peer to stop sending on its data stream.
    StopData {
        /// The stream.
        stream: StreamRef,
        /// The STOP_SENDING code.
        error_code: u64,
    },
    /// Open a bidirectional stream and write its first bytes.
    OpenBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The first bytes.
        bytes: &'session [u8],
        /// Close the local send half after them.
        fin: bool,
    },
    /// Write on a bidirectional stream.
    SendBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The bytes.
        bytes: &'session [u8],
        /// Close the local send half after them.
        fin: bool,
    },
    /// Close the local send half of a bidirectional stream (FIN).
    CloseBidiStream {
        /// The stream.
        stream: StreamRef,
    },
    /// Send a datagram.
    SendDatagram {
        /// The datagram.
        bytes: &'session [u8],
    },
    /// Open this side's unidirectional control stream with its first bytes.
    OpenUniControl {
        /// The stream.
        stream: StreamRef,
        /// The first bytes.
        bytes: &'session [u8],
    },
    /// Write on this side's unidirectional control stream. The current
    /// core does not emit it (later control messages come as
    /// [`SendControl`](ActionDetail::SendControl)); a host that receives one
    /// treats it as `SendControl`.
    SendUniControl {
        /// The stream.
        stream: StreamRef,
        /// The bytes.
        bytes: &'session [u8],
        /// End the stream (teardown only).
        fin: bool,
    },
    /// Reset the local send half of a bidirectional stream.
    ResetBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The reset code.
        error_code: u64,
    },
    /// Ask the peer to stop sending on a bidirectional stream.
    StopBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The STOP_SENDING code.
        error_code: u64,
    },
    /// Abort both directions of a bidirectional stream with one code.
    AbortBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The code for both directions.
        error_code: u64,
    },
    /// A data stream's scheduling key, before its first data.
    SetDataPriority {
        /// The stream; `None` updates every stream of `priority.owner`.
        stream: Option<StreamRef>,
        /// The key.
        priority: DataPriority,
    },
    /// A kind this crate does not know. A host cannot skip it: a later
    /// action may depend on it.
    Unknown {
        /// The native kind.
        kind: u32,
    },
}

/// The owned copy of an action, from [`Action::into_owned`]: every borrowed
/// span copied, the payload a [`Buffer`] of its own. It no longer borrows
/// the session.
#[non_exhaustive]
#[derive(Debug)]
pub enum OwnedActionDetail {
    /// See [`ActionDetail::SendControl`].
    SendControl {
        /// The control bytes.
        bytes: Vec<u8>,
    },
    /// See [`ActionDetail::CloseSession`].
    CloseSession {
        /// The session termination code.
        code: u64,
        /// The reason phrase.
        reason: Vec<u8>,
    },
    /// See [`ActionDetail::SendData`].
    SendData {
        /// The stream.
        stream: StreamRef,
        /// Header bytes written before the payload.
        header: StreamHeader,
        /// The payload, if any.
        payload: Option<Buffer>,
        /// End the stream after these bytes.
        fin: bool,
        /// See [`ActionDetail::SendData`].
        scheduling_priority: Option<u64>,
    },
    /// See [`ActionDetail::ResetData`].
    ResetData {
        /// The stream.
        stream: StreamRef,
        /// The reset code.
        error_code: u64,
        /// Leading bytes the reset must still deliver.
        reliable_size: Option<u64>,
    },
    /// See [`ActionDetail::StopData`].
    StopData {
        /// The stream.
        stream: StreamRef,
        /// The STOP_SENDING code.
        error_code: u64,
    },
    /// See [`ActionDetail::OpenBidiStream`].
    OpenBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The first bytes.
        bytes: Vec<u8>,
        /// Close the local send half after them.
        fin: bool,
    },
    /// See [`ActionDetail::SendBidiStream`].
    SendBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The bytes.
        bytes: Vec<u8>,
        /// Close the local send half after them.
        fin: bool,
    },
    /// See [`ActionDetail::CloseBidiStream`].
    CloseBidiStream {
        /// The stream.
        stream: StreamRef,
    },
    /// See [`ActionDetail::SendDatagram`].
    SendDatagram {
        /// The datagram.
        bytes: Vec<u8>,
    },
    /// See [`ActionDetail::OpenUniControl`].
    OpenUniControl {
        /// The stream.
        stream: StreamRef,
        /// The first bytes.
        bytes: Vec<u8>,
    },
    /// See [`ActionDetail::SendUniControl`].
    SendUniControl {
        /// The stream.
        stream: StreamRef,
        /// The bytes.
        bytes: Vec<u8>,
        /// End the stream.
        fin: bool,
    },
    /// See [`ActionDetail::ResetBidiStream`].
    ResetBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The reset code.
        error_code: u64,
    },
    /// See [`ActionDetail::StopBidiStream`].
    StopBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The STOP_SENDING code.
        error_code: u64,
    },
    /// See [`ActionDetail::AbortBidiStream`].
    AbortBidiStream {
        /// The stream.
        stream: StreamRef,
        /// The code for both directions.
        error_code: u64,
    },
    /// See [`ActionDetail::SetDataPriority`].
    SetDataPriority {
        /// The stream, or every stream of the owner.
        stream: Option<StreamRef>,
        /// The key.
        priority: DataPriority,
    },
    /// See [`ActionDetail::Unknown`].
    Unknown {
        /// The native kind.
        kind: u32,
    },
}

/// The polled record, released (`moq_action_cleanup`) once it is decoded,
/// or when decoding fails. The payload is moved out first, so its release
/// runs once either way: by the payload, or by this cleanup.
struct RawAction(sys::moq_action_t);

impl Drop for RawAction {
    fn drop(&mut self) {
        // SAFETY: a record the session wrote; cleanup is idempotent and
        // releases only what the record still owns (its payload field was
        // cleared when the payload was adopted).
        unsafe { sys::moq_action_cleanup(&mut self.0) }
    }
}

/// One polled action. It borrows the session exclusively: no advancing call
/// can run while it is alive, so its borrowed bytes stay valid. It is
/// neither `Send` nor `Sync`. Drop it once the transport took it whole, or
/// convert it with [`into_owned`](Action::into_owned) to hold what the
/// transport could not take yet.
pub struct Action<'session> {
    kind: ActionKind,
    tag: Option<ActionTag>,
    detail: ActionDetail<'session>,
    _session: PhantomData<(&'session mut (), *const ())>,
}

// Test controls: while set, a copy of that kind fails as a failed
// allocation would. Compiled into the crate's own tests only.
#[cfg(test)]
thread_local! {
    static FAIL_SPAN_COPY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static FAIL_PAYLOAD_COPY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// A copy of `bytes` whose allocation failure is reported, not aborted on.
fn copy(bytes: &[u8]) -> Result<Vec<u8>> {
    #[cfg(test)]
    if FAIL_SPAN_COPY.with(std::cell::Cell::get) {
        return Err(Error::NOMEM);
    }
    let mut out = Vec::new();
    out.try_reserve_exact(bytes.len())
        .map_err(|_| Error::NOMEM)?;
    out.extend_from_slice(bytes);
    Ok(out)
}

/// A payload's bytes in storage of its own.
fn copy_payload(payload: &Payload<'_>) -> Result<Buffer> {
    #[cfg(test)]
    if FAIL_PAYLOAD_COPY.with(std::cell::Cell::get) {
        return Err(Error::NOMEM);
    }
    Buffer::copy_from(payload.as_bytes())
}

impl<'session> Action<'session> {
    /// Convert a polled record. On failure the record is released and the
    /// error says why.
    ///
    /// # Safety
    /// `raw` was written by a poll of a session that stays exclusively
    /// borrowed for `'session`; the caller owns its references.
    pub(super) unsafe fn from_raw(
        raw: sys::moq_action_t,
        tag: Option<&sys::moq_action_tag_t>,
    ) -> Result<Action<'session>> {
        let mut record = RawAction(raw);
        let tag = tag.map(ActionTag::from_raw).transpose()?;
        let kind = ActionKind::from_code(record.0.kind);
        // SAFETY: the caller's contract: the record's borrowed spans point
        // into the session's send buffer or static storage, valid for
        // 'session.
        let detail = unsafe { detail(&mut record.0, kind) }?;
        // The payload was moved out of the record; releasing it now frees
        // only what a kind this crate does not know may still hold.
        drop(record);
        Ok(Action {
            kind,
            tag,
            detail,
            _session: PhantomData,
        })
    }

    /// The kind.
    #[must_use]
    pub fn kind(&self) -> ActionKind {
        self.kind
    }

    /// The tag, in the tagged mode.
    #[must_use]
    pub fn tag(&self) -> Option<&ActionTag> {
        self.tag.as_ref()
    }

    /// What the action asks.
    #[must_use]
    pub fn detail(&self) -> &ActionDetail<'session> {
        &self.detail
    }

    /// What the action asks, by value (to take its payload).
    #[must_use]
    pub fn into_detail(self) -> ActionDetail<'session> {
        self.detail
    }

    /// The transport stream the action is for, if it names one.
    #[must_use]
    pub fn stream(&self) -> Option<StreamRef> {
        self.detail.stream()
    }

    /// The action's place in the output order (see [`OrderKey`]).
    #[must_use]
    pub fn order_key(&self) -> OrderKey {
        self.detail.order_key()
    }

    /// Hold the action past the session's next advancing call: copy every
    /// borrowed span and the payload, then release the original.
    ///
    /// Atomic: every copy is made first and the payload last; if any fails,
    /// what was copied is freed and the action comes back untouched with the
    /// error, still borrowing the session.
    ///
    /// # Errors
    /// [`Error::NOMEM`] when a copy cannot be allocated, with the action.
    // The action comes back whole on the one failure, which is allocation:
    // boxing it would allocate on that very path.
    #[allow(clippy::result_large_err)]
    pub fn into_owned(self) -> std::result::Result<OwnedAction, (Action<'session>, Error)> {
        let owned = match self.owned_detail() {
            Ok(detail) => detail,
            Err(error) => return Err((self, error)),
        };
        Ok(OwnedAction {
            kind: self.kind,
            tag: self.tag,
            detail: owned,
        })
        // `self` drops here: its payload reference and record are released.
    }

    fn owned_detail(&self) -> Result<OwnedActionDetail> {
        Ok(match &self.detail {
            ActionDetail::SendControl { bytes } => OwnedActionDetail::SendControl {
                bytes: copy(bytes)?,
            },
            ActionDetail::CloseSession { code, reason } => OwnedActionDetail::CloseSession {
                code: *code,
                reason: copy(reason)?,
            },
            ActionDetail::SendData {
                stream,
                header,
                payload,
                fin,
                scheduling_priority,
            } => OwnedActionDetail::SendData {
                stream: *stream,
                header: *header,
                // The payload is the only copy here: it is the last step.
                payload: match payload {
                    Some(payload) => Some(copy_payload(payload)?),
                    None => None,
                },
                fin: *fin,
                scheduling_priority: *scheduling_priority,
            },
            ActionDetail::ResetData {
                stream,
                error_code,
                reliable_size,
            } => OwnedActionDetail::ResetData {
                stream: *stream,
                error_code: *error_code,
                reliable_size: *reliable_size,
            },
            ActionDetail::StopData { stream, error_code } => OwnedActionDetail::StopData {
                stream: *stream,
                error_code: *error_code,
            },
            ActionDetail::OpenBidiStream { stream, bytes, fin } => {
                OwnedActionDetail::OpenBidiStream {
                    stream: *stream,
                    bytes: copy(bytes)?,
                    fin: *fin,
                }
            }
            ActionDetail::SendBidiStream { stream, bytes, fin } => {
                OwnedActionDetail::SendBidiStream {
                    stream: *stream,
                    bytes: copy(bytes)?,
                    fin: *fin,
                }
            }
            ActionDetail::CloseBidiStream { stream } => {
                OwnedActionDetail::CloseBidiStream { stream: *stream }
            }
            ActionDetail::SendDatagram { bytes } => OwnedActionDetail::SendDatagram {
                bytes: copy(bytes)?,
            },
            ActionDetail::OpenUniControl { stream, bytes } => OwnedActionDetail::OpenUniControl {
                stream: *stream,
                bytes: copy(bytes)?,
            },
            ActionDetail::SendUniControl { stream, bytes, fin } => {
                OwnedActionDetail::SendUniControl {
                    stream: *stream,
                    bytes: copy(bytes)?,
                    fin: *fin,
                }
            }
            ActionDetail::ResetBidiStream { stream, error_code } => {
                OwnedActionDetail::ResetBidiStream {
                    stream: *stream,
                    error_code: *error_code,
                }
            }
            ActionDetail::StopBidiStream { stream, error_code } => {
                OwnedActionDetail::StopBidiStream {
                    stream: *stream,
                    error_code: *error_code,
                }
            }
            ActionDetail::AbortBidiStream { stream, error_code } => {
                OwnedActionDetail::AbortBidiStream {
                    stream: *stream,
                    error_code: *error_code,
                }
            }
            ActionDetail::SetDataPriority { stream, priority } => {
                OwnedActionDetail::SetDataPriority {
                    stream: *stream,
                    priority: *priority,
                }
            }
            ActionDetail::Unknown { kind } => OwnedActionDetail::Unknown { kind: *kind },
        })
    }
}

impl fmt::Debug for Action<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Action")
            .field("tag", &self.tag)
            .field("detail", &self.detail)
            .finish()
    }
}

impl ActionDetail<'_> {
    /// The transport stream the action is for, if it names one.
    #[must_use]
    pub fn stream(&self) -> Option<StreamRef> {
        match self {
            ActionDetail::SendData { stream, .. }
            | ActionDetail::ResetData { stream, .. }
            | ActionDetail::StopData { stream, .. }
            | ActionDetail::OpenBidiStream { stream, .. }
            | ActionDetail::SendBidiStream { stream, .. }
            | ActionDetail::CloseBidiStream { stream }
            | ActionDetail::OpenUniControl { stream, .. }
            | ActionDetail::SendUniControl { stream, .. }
            | ActionDetail::ResetBidiStream { stream, .. }
            | ActionDetail::StopBidiStream { stream, .. }
            | ActionDetail::AbortBidiStream { stream, .. } => Some(*stream),
            ActionDetail::SetDataPriority { stream, .. } => *stream,
            ActionDetail::SendControl { .. }
            | ActionDetail::CloseSession { .. }
            | ActionDetail::SendDatagram { .. }
            | ActionDetail::Unknown { .. } => None,
        }
    }

    /// The action's place in the output order (see [`OrderKey`]).
    #[must_use]
    pub fn order_key(&self) -> OrderKey {
        match self {
            ActionDetail::SendControl { .. }
            | ActionDetail::OpenUniControl { .. }
            | ActionDetail::SendUniControl { .. } => OrderKey::Control,
            ActionDetail::SendDatagram { .. } => OrderKey::Datagrams,
            ActionDetail::CloseSession { .. } | ActionDetail::Unknown { .. } => OrderKey::Session,
            other => other.stream().map_or(OrderKey::Session, OrderKey::Stream),
        }
    }
}

/// An action that no longer borrows the session (see [`Action::into_owned`]).
/// It holds independent copies, so it may move between threads.
#[derive(Debug)]
pub struct OwnedAction {
    kind: ActionKind,
    tag: Option<ActionTag>,
    detail: OwnedActionDetail,
}

impl OwnedAction {
    /// The kind.
    #[must_use]
    pub fn kind(&self) -> ActionKind {
        self.kind
    }

    /// The tag, in the tagged mode.
    #[must_use]
    pub fn tag(&self) -> Option<&ActionTag> {
        self.tag.as_ref()
    }

    /// What the action asks.
    #[must_use]
    pub fn detail(&self) -> &OwnedActionDetail {
        &self.detail
    }

    /// What the action asks, by value.
    #[must_use]
    pub fn into_detail(self) -> OwnedActionDetail {
        self.detail
    }

    /// The transport stream the action is for, if it names one.
    #[must_use]
    pub fn stream(&self) -> Option<StreamRef> {
        match &self.detail {
            OwnedActionDetail::SendData { stream, .. }
            | OwnedActionDetail::ResetData { stream, .. }
            | OwnedActionDetail::StopData { stream, .. }
            | OwnedActionDetail::OpenBidiStream { stream, .. }
            | OwnedActionDetail::SendBidiStream { stream, .. }
            | OwnedActionDetail::CloseBidiStream { stream }
            | OwnedActionDetail::OpenUniControl { stream, .. }
            | OwnedActionDetail::SendUniControl { stream, .. }
            | OwnedActionDetail::ResetBidiStream { stream, .. }
            | OwnedActionDetail::StopBidiStream { stream, .. }
            | OwnedActionDetail::AbortBidiStream { stream, .. } => Some(*stream),
            OwnedActionDetail::SetDataPriority { stream, .. } => *stream,
            OwnedActionDetail::SendControl { .. }
            | OwnedActionDetail::CloseSession { .. }
            | OwnedActionDetail::SendDatagram { .. }
            | OwnedActionDetail::Unknown { .. } => None,
        }
    }

    /// The action's place in the output order (see [`OrderKey`]).
    #[must_use]
    pub fn order_key(&self) -> OrderKey {
        match &self.detail {
            OwnedActionDetail::SendControl { .. }
            | OwnedActionDetail::OpenUniControl { .. }
            | OwnedActionDetail::SendUniControl { .. } => OrderKey::Control,
            OwnedActionDetail::SendDatagram { .. } => OrderKey::Datagrams,
            OwnedActionDetail::CloseSession { .. } | OwnedActionDetail::Unknown { .. } => {
                OrderKey::Session
            }
            _ => self.stream().map_or(OrderKey::Session, OrderKey::Stream),
        }
    }
}

/// Decode one record's detail by kind, checking the kind's frozen floor and
/// the coverage of every appended field read.
///
/// # Safety
/// As for [`Action::from_raw`].
unsafe fn detail<'session>(
    raw: &mut sys::moq_action_t,
    kind: ActionKind,
) -> Result<ActionDetail<'session>> {
    let size = raw.detail_size;
    // SAFETY: the record's kind selects the union member C wrote, and each
    // kind's floor check precedes its read; every borrowed span is valid for
    // 'session by the caller's contract; the payload is the record's own
    // reference, transferred on poll, and its field is cleared as it is
    // adopted so the record no longer releases it.
    Ok(unsafe {
        match kind {
            ActionKind::SendControl => {
                raw::floor(size, sys::MOQ_SEND_CONTROL_ACTION_V0_SIZE)?;
                let d = raw.u.send_control;
                ActionDetail::SendControl {
                    bytes: raw::record_bytes(d.data, d.len)?,
                }
            }
            ActionKind::CloseSession => {
                raw::floor(size, sys::MOQ_CLOSE_SESSION_ACTION_V0_SIZE)?;
                let d = raw.u.close_session;
                ActionDetail::CloseSession {
                    code: d.code,
                    reason: raw::record_span(d.reason)?,
                }
            }
            ActionKind::SendData => {
                raw::floor(size, sys::MOQ_SEND_DATA_ACTION_V0_SIZE)?;
                let d = raw.u.send_data;
                let stream = StreamRef::from_record(d.stream_ref)?;
                if usize::from(d.header_len) > d.header.len() {
                    return Err(Error::ABI_MISMATCH);
                }
                let scheduling_priority = if raw::covers(
                    size,
                    field_end!(sys::moq_send_data_action_t, scheduling_priority, u64),
                ) && d.scheduling_priority != 0
                {
                    Some(d.scheduling_priority)
                } else {
                    None
                };
                // Take the owned reference out of the record last, after every
                // check: from here the payload, not the record, releases it.
                let payload = Payload::adopt(d.payload);
                raw.u.send_data.payload = std::ptr::null_mut();
                ActionDetail::SendData {
                    stream,
                    header: StreamHeader {
                        bytes: d.header,
                        len: d.header_len,
                    },
                    payload,
                    fin: d.fin,
                    scheduling_priority,
                }
            }
            ActionKind::ResetData => {
                raw::floor(size, sys::MOQ_RESET_DATA_ACTION_V0_SIZE)?;
                let d = raw.u.reset_data;
                ActionDetail::ResetData {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    error_code: d.error_code,
                    reliable_size: raw::covers(
                        size,
                        field_end!(sys::moq_reset_data_action_t, reliable_size, u64),
                    )
                    .then_some(d.reliable_size),
                }
            }
            ActionKind::StopData => {
                raw::floor(size, sys::MOQ_STOP_DATA_ACTION_V0_SIZE)?;
                let d = raw.u.stop_data;
                ActionDetail::StopData {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    error_code: d.error_code,
                }
            }
            ActionKind::OpenBidiStream => {
                raw::floor(size, sys::MOQ_OPEN_BIDI_STREAM_ACTION_V0_SIZE)?;
                let d = raw.u.open_bidi_stream;
                ActionDetail::OpenBidiStream {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    bytes: raw::record_bytes(d.data, d.len)?,
                    fin: d.fin,
                }
            }
            ActionKind::SendBidiStream => {
                raw::floor(size, sys::MOQ_SEND_BIDI_STREAM_ACTION_V0_SIZE)?;
                let d = raw.u.send_bidi_stream;
                ActionDetail::SendBidiStream {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    bytes: raw::record_bytes(d.data, d.len)?,
                    fin: d.fin,
                }
            }
            ActionKind::CloseBidiStream => {
                raw::floor(size, sys::MOQ_CLOSE_BIDI_STREAM_ACTION_V0_SIZE)?;
                let d = raw.u.close_bidi_stream;
                ActionDetail::CloseBidiStream {
                    stream: StreamRef::from_record(d.stream_ref)?,
                }
            }
            ActionKind::SendDatagram => {
                raw::floor(size, sys::MOQ_SEND_DATAGRAM_ACTION_V0_SIZE)?;
                let d = raw.u.send_datagram;
                ActionDetail::SendDatagram {
                    bytes: raw::record_bytes(d.data, d.len)?,
                }
            }
            ActionKind::OpenUniControl => {
                raw::floor(size, sys::MOQ_OPEN_UNI_CONTROL_ACTION_V0_SIZE)?;
                let d = raw.u.open_uni_control;
                ActionDetail::OpenUniControl {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    bytes: raw::record_bytes(d.data, d.len)?,
                }
            }
            ActionKind::SendUniControl => {
                raw::floor(size, sys::MOQ_SEND_UNI_CONTROL_ACTION_V0_SIZE)?;
                let d = raw.u.send_uni_control;
                ActionDetail::SendUniControl {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    bytes: raw::record_bytes(d.data, d.len)?,
                    fin: d.fin,
                }
            }
            ActionKind::ResetBidiStream => {
                raw::floor(size, sys::MOQ_RESET_BIDI_STREAM_ACTION_V0_SIZE)?;
                let d = raw.u.reset_bidi_stream;
                ActionDetail::ResetBidiStream {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    error_code: d.error_code,
                }
            }
            ActionKind::StopBidiStream => {
                raw::floor(size, sys::MOQ_STOP_BIDI_STREAM_ACTION_V0_SIZE)?;
                let d = raw.u.stop_bidi_stream;
                ActionDetail::StopBidiStream {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    error_code: d.error_code,
                }
            }
            ActionKind::AbortBidiStream => {
                raw::floor(size, sys::MOQ_ABORT_BIDI_STREAM_ACTION_V0_SIZE)?;
                let d = raw.u.abort_bidi_stream;
                ActionDetail::AbortBidiStream {
                    stream: StreamRef::from_record(d.stream_ref)?,
                    error_code: d.error_code,
                }
            }
            ActionKind::SetDataPriority => {
                raw::floor(size, sys::MOQ_SET_DATA_PRIORITY_ACTION_V0_SIZE)?;
                let d = raw.u.set_data_priority;
                ActionDetail::SetDataPriority {
                    stream: StreamRef::from_raw(d.stream_ref._v),
                    priority: DataPriority {
                        subscriber_priority: d.priority.subscriber_priority,
                        publisher_priority: d.priority.publisher_priority,
                        descending: d.priority.descending,
                        owner: d.priority.owner,
                        group_id: d.priority.group_id,
                        subgroup_id: d.priority.subgroup_id,
                    },
                }
            }
            ActionKind::Other(kind) => ActionDetail::Unknown { kind },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_native_kind_has_a_variant() {
        for code in 1..=15 {
            let kind = ActionKind::from_code(code);
            assert!(!matches!(kind, ActionKind::Other(_)), "action kind {code}");
            assert_eq!(kind.code(), code);
        }
        assert_eq!(ActionKind::from_code(99), ActionKind::Other(99));
        assert_eq!(ActionKind::Other(99).code(), 99);
    }

    #[test]
    fn a_short_record_is_refused() {
        // SAFETY: zero is a valid record; its kind's floor is not met.
        let mut raw: sys::moq_action_t = unsafe { std::mem::zeroed() };
        raw.kind = sys::MOQ_ACTION_RESET_DATA;
        raw.detail_size = 4;
        // SAFETY: a record with no borrowed span and no owned reference.
        let result = unsafe { Action::from_raw(raw, None) };
        assert_eq!(result.err(), Some(Error::ABI_MISMATCH));
    }

    #[test]
    fn copies_report_failure() {
        assert_eq!(copy(b"abc"), Ok(b"abc".to_vec()));
        assert_eq!(copy(&[]), Ok(Vec::new()));
    }

    fn failing<T>(
        control: &'static std::thread::LocalKey<std::cell::Cell<bool>>,
        f: impl FnOnce() -> T,
    ) -> T {
        control.with(|c| c.set(true));
        let result = f();
        control.with(|c| c.set(false));
        result
    }

    #[test]
    fn a_failed_span_copy_returns_the_action_untouched() {
        use crate::session::{Perspective, Session, SessionConfig};
        use crate::Version;
        let config = SessionConfig::new(Perspective::Client, Version::DRAFT_16);
        let mut session = Session::try_new(&config, Instant::from_micros(1)).expect("a session");
        session.start(Instant::from_micros(1)).expect("start");
        let ActionPoll::Ready(action) = session.poll_action().expect("a poll") else {
            panic!("the setup is an action");
        };
        let ActionDetail::SendControl { bytes } = action.detail() else {
            panic!("the setup is control bytes");
        };
        let (at, snapshot) = (bytes.as_ptr(), bytes.to_vec());
        let kind = action.kind();
        let (action, error) =
            failing(&FAIL_SPAN_COPY, || action.into_owned().err()).expect("the copy fails");
        assert_eq!(error, Error::NOMEM);
        assert_eq!(action.kind(), kind);
        let ActionDetail::SendControl { bytes } = action.detail() else {
            panic!("the same detail");
        };
        assert_eq!(
            (bytes.as_ptr(), bytes.to_vec()),
            (at, snapshot.clone()),
            "untouched"
        );
        let owned = action.into_owned().expect("then the copy succeeds");
        // The session advances; the copy is the action's own.
        session
            .close(0, "end", Instant::from_micros(2))
            .expect("close");
        while let ActionPoll::Ready(next) = session.poll_action().expect("a poll") {
            drop(next);
        }
        let OwnedActionDetail::SendControl { bytes } = owned.detail() else {
            panic!("an owned copy of the detail");
        };
        assert_eq!(bytes, &snapshot);
        assert_ne!(bytes.as_ptr(), at);
    }

    static RELEASED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

    unsafe extern "C" fn count_release(
        _context: *mut std::ffi::c_void,
        _data: *const u8,
        _len: usize,
    ) {
        RELEASED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    #[test]
    fn a_failed_payload_copy_keeps_the_payload_reference() {
        static BYTES: [u8; 6] = *b"object";
        // SAFETY: static bytes; the release only counts.
        let payload = unsafe {
            Buffer::wrap_raw(
                BYTES.as_ptr(),
                BYTES.len(),
                Some(count_release),
                std::ptr::null_mut(),
            )
        }
        .expect("a payload");
        let reference = payload.into_raw();
        // SAFETY: zero is a valid record.
        let mut raw: sys::moq_action_t = unsafe { std::mem::zeroed() };
        raw.kind = sys::MOQ_ACTION_SEND_DATA;
        raw.detail_size = u32::try_from(std::mem::size_of::<sys::moq_send_data_action_t>())
            .expect("a small record");
        // SAFETY: the union member the kind selects.
        unsafe {
            raw.u.send_data.stream_ref = sys::moq_stream_ref_t { _v: 4 };
            raw.u.send_data.header[..3].copy_from_slice(&[1, 2, 3]);
            raw.u.send_data.header_len = 3;
            raw.u.send_data.payload = reference;
            raw.u.send_data.fin = true;
        }
        // SAFETY: zero is a valid tag.
        let mut tag: sys::moq_action_tag_t = unsafe { std::mem::zeroed() };
        tag.struct_size = u32::try_from(std::mem::size_of_val(&tag)).expect("small");
        tag.flags = sys::MOQ_ACTION_TAG_FIRST_OF_STREAM;
        tag.owner._v = 7;
        tag.deadline_us = u64::MAX;
        // SAFETY: the record owns the one reference to the payload and
        // borrows nothing.
        let action: Action<'static> =
            unsafe { Action::from_raw(raw, Some(&tag)) }.expect("decodes");
        let before = RELEASED.load(std::sync::atomic::Ordering::SeqCst);
        let (action, error) = failing(&FAIL_PAYLOAD_COPY, || action.into_owned().err())
            .expect("the payload copy fails");
        assert_eq!(error, Error::NOMEM);
        // SAFETY: the action still holds the reference.
        let count = unsafe { sys::moq_rcbuf_refcount(reference) };
        assert_eq!(count, 1, "no reference added or lost");
        assert_eq!(
            RELEASED.load(std::sync::atomic::Ordering::SeqCst),
            before,
            "not released early"
        );
        let ActionDetail::SendData {
            header,
            payload,
            fin,
            ..
        } = action.detail()
        else {
            panic!("the same detail");
        };
        assert_eq!(header.as_bytes(), &[1, 2, 3]);
        assert!(*fin);
        assert_eq!(
            payload.as_ref().map(Payload::as_bytes),
            Some(&b"object"[..])
        );
        let kept_tag = action.tag().expect("its tag");
        assert!(kept_tag.first_of_stream());
        assert_eq!(kept_tag.owner().map(OutputOwner::as_raw), Some(7));
        let owned = action.into_owned().expect("then the copy succeeds");
        assert_eq!(
            RELEASED.load(std::sync::atomic::Ordering::SeqCst),
            before + 1,
            "the record's reference released once, on success"
        );
        let OwnedActionDetail::SendData {
            payload,
            header,
            fin,
            ..
        } = owned.detail()
        else {
            panic!("an owned copy of the detail");
        };
        let copy = payload.as_ref().expect("the copy");
        assert_eq!(copy.as_slice(), b"object");
        assert_ne!(copy.as_ptr(), reference);
        assert_eq!((header.as_bytes(), *fin), (&[1, 2, 3][..], true));
    }
}
