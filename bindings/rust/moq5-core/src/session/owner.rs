//! The session owner: lifecycle, inputs and outputs.

use std::ffi::{c_void, CString};
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ptr::{self, NonNull};

use super::action::{Action, ActionPoll, OutputOwner};
use super::binding::{BindRefused, FactSet, PolicyCell, TransportBinding};
use super::config::{Perspective, SessionConfig, SessionState};
use super::custody::{
    Fed, FeedError, NotAdmitted, NotAdmittedReason, OwedSignal, PeerSignal, SignalError,
};
use super::event::{self, Event, RawEvent};
use super::handle::OwnerId;
use super::host::{
    DataEnd, DataOpenOutcome, DataResetClaim, DataResetOffer, PrefixFact, ResetOfferPoll,
    Retirement, Tick, UniRoute,
};
use super::raw;
use super::stream::StreamRef;
use super::time::Instant;
use crate::{sys, Buffer, Error, Result, Version};

/// The owner of one sans-I/O MoQ session (`moq_session_t`).
///
/// A deterministic state machine: the host feeds it transport input and
/// time, and polls actions (what to do on the transport) and events (what
/// happened) out of it. It reads no clock, opens no socket and starts no
/// thread. See the crate documentation for the host contract.
///
/// **Threads.** The owner is neither `Send` nor `Sync`: the native session
/// is confined to the thread that created it, and no written native contract
/// yet allows a whole session to move between threads. Every method takes
/// `&mut self` unless the native call only observes.
///
/// **Borrows.** A polled [`Action`] or [`Event`] borrows the owner
/// exclusively, so no advancing call can run while it is alive: the bytes
/// it borrows stay valid for exactly as long as Rust lets them be used.
///
/// **Teardown.** Dropping the owner destroys the session; what it still
/// queued is released. The URI policy and the close reason C borrows are
/// dropped after it.
pub struct Session {
    raw: NonNull<sys::moq_session_t>,
    pub(super) owner: OwnerId,
    bound: bool,
    /// Borrowed by C until the session is destroyed.
    policy: Option<Box<PolicyCell>>,
    /// Borrowed by C's close outputs until they are polled; kept until the
    /// session is destroyed.
    close_reason: Option<CString>,
    _confined: PhantomData<*mut ()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        // SAFETY: the session this owner created, destroyed once. No polled
        // record outlives the owner (each borrows it), and the policy cell
        // and close reason C borrows are fields, dropped after this.
        unsafe { sys::moq_session_destroy(self.raw.as_ptr()) }
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Session")
            .field("version", &self.version())
            .field("perspective", &self.perspective())
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

fn data_ptr(bytes: &[u8]) -> *const u8 {
    if bytes.is_empty() {
        ptr::null()
    } else {
        bytes.as_ptr()
    }
}

impl Session {
    /// Create a session (`moq_session_create`). It starts idle: a draft-16
    /// client and every draft-18 and draft-22 side call
    /// [`start`](Session::start); a draft-16 server waits for the peer's
    /// setup.
    ///
    /// # Errors
    /// The native code: [`Error::INVAL`] for a configuration C refuses (a
    /// version this build cannot speak, a timeout that is not a whole number
    /// of microseconds), [`Error::NOMEM`].
    pub fn try_new(config: &SessionConfig, now: Instant) -> Result<Session> {
        let native = config.raw()?;
        let mut out = ptr::null_mut();
        // SAFETY: a configuration built by its sized initializer, borrowed
        // for the call with the token records and values it points at; C
        // copies what it keeps, the setup tokens included.
        raw::check(unsafe { sys::moq_session_create(&native.cfg, now.as_micros(), &mut out) })?;
        let raw = NonNull::new(out).ok_or(Error::INTERNAL)?;
        Ok(Session {
            raw,
            owner: OwnerId::next(),
            bound: false,
            policy: None,
            close_reason: None,
            _confined: PhantomData,
        })
    }

    /// Whether this build can offer `version` to a peer.
    #[must_use]
    pub fn version_supported(version: Version) -> bool {
        // SAFETY: a pure query.
        unsafe { sys::moq_version_supported(version.as_raw()) }
    }

    pub(super) fn ptr(&self) -> *mut sys::moq_session_t {
        self.raw.as_ptr()
    }

    /// The wire version, fixed at creation.
    #[must_use]
    pub fn version(&self) -> Version {
        // SAFETY: an observing call on a live session.
        Version::from_raw(unsafe { sys::moq_session_version(self.ptr()) })
    }

    /// This side's perspective.
    #[must_use]
    pub fn perspective(&self) -> Perspective {
        // SAFETY: an observing call on a live session.
        match unsafe { sys::moq_session_perspective(self.ptr()) } {
            sys::MOQ_PERSPECTIVE_SERVER => Perspective::Server,
            _ => Perspective::Client,
        }
    }

    /// The session's state.
    #[must_use]
    pub fn state(&self) -> SessionState {
        // SAFETY: an observing call on a live session.
        SessionState::from_raw(unsafe { sys::moq_session_state(self.ptr()) })
    }

    /// Whether the session is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.state() == SessionState::Closed
    }

    // -- Binding and start ------------------------------------------------

    /// The facts the profile requires before it starts; empty when it
    /// starts without a binding (drafts 16 and 18).
    #[must_use]
    pub fn binding_requirements(&self) -> FactSet {
        // SAFETY: an observing call on a live session.
        FactSet::from_bits(unsafe { sys::moq_session_binding_requirements(self.ptr()) })
    }

    /// Whether the profile requires a binding this session does not have.
    fn binding_missing(&self) -> bool {
        !self.bound && !self.binding_requirements().is_empty()
    }

    /// Bind the transport (`moq_session_bind_transport`), before start and
    /// before any input. Single use. C copies the target; a server's URI
    /// policy moves into the owner and is dropped after the session.
    ///
    /// # Errors
    /// A refusal, with the binding back whole: C kept nothing and the
    /// session is unchanged, so a binding refused for a fact not yet known
    /// can be updated ([`TransportBinding::facts_mut`]) and bound again.
    pub fn bind_transport(
        &mut self,
        binding: TransportBinding,
    ) -> std::result::Result<(), BindRefused> {
        let prepared = binding.prepare();
        // SAFETY: the record points into `prepared`'s target bytes or policy
        // cell, alive and unmoved for the call; its size is passed exactly.
        let rc = unsafe {
            sys::moq_session_bind_transport(
                self.ptr(),
                &prepared.raw,
                mem::size_of::<sys::moq_transport_binding_t>(),
            )
        };
        if rc == sys::MOQ_OK {
            self.bound = true;
            self.policy = prepared.into_policy();
            Ok(())
        } else {
            Err(prepared.refused(Error::from_code(rc)))
        }
    }

    /// Enable the tagged host mode (`moq_session_enable_tagged_output`) on
    /// a pristine session: after creation and binding, before start or any
    /// other advancing call. Permanent. In it, actions carry
    /// [`ActionTag`](crate::ActionTag)s, counted opens wait for
    /// [`report_data_open`](Session::report_data_open), every FIN of a local
    /// data stream the transport accepted is reported with
    /// [`report_data_fin`](Session::report_data_fin), and data resets of
    /// opened streams go through the reset transaction.
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] once the session is no longer pristine.
    pub fn enable_tagged_output(&mut self) -> Result<()> {
        // SAFETY: a live session.
        raw::check(unsafe { sys::moq_session_enable_tagged_output(self.ptr()) })
    }

    /// Whether the tagged host mode is on.
    #[must_use]
    pub fn tagged_output(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_tagged_output(self.ptr()) }
    }

    /// Begin the setup (`moq_session_start`).
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] when the session is not idle, for a draft-16
    /// server, or while a required binding is missing; [`Error::BUFFER`]
    /// when this side's setup does not fit its send buffer (creation checked
    /// that it encodes, not that it fits); [`Error::NOMEM`] when the setup
    /// cannot be encoded for want of memory.
    pub fn start(&mut self, now: Instant) -> Result<()> {
        // SAFETY: an advancing call on a live session; no record borrows it.
        raw::check(unsafe { sys::moq_session_start(self.ptr(), now.as_micros()) })
    }

    /// Close the session locally (`moq_session_close`): everything still
    /// queued is superseded by one close action and one close event. The
    /// reason is copied into the owner, which keeps it until the session is
    /// destroyed (C borrows it from the close outputs). On a closed session
    /// this changes nothing.
    ///
    /// # Errors
    /// [`Error::INVAL`] for a reason containing a NUL byte.
    pub fn close(&mut self, code: u64, reason: &str, now: Instant) -> Result<()> {
        if reason.contains('\0') {
            return Err(Error::INVAL);
        }
        if self.is_closed() || reason.is_empty() {
            // C ignores the reason of a second close; an empty one is none.
            // SAFETY: an advancing call on a live session.
            return raw::check(unsafe {
                sys::moq_session_close(self.ptr(), code, ptr::null(), now.as_micros())
            });
        }
        let reason = CString::new(reason).map_err(|_| Error::INVAL)?;
        // SAFETY: the reason stays alive and unmoved in the owner (a
        // CString's bytes are on the heap) until the session is destroyed.
        let rc =
            unsafe { sys::moq_session_close(self.ptr(), code, reason.as_ptr(), now.as_micros()) };
        raw::check(rc)?;
        self.close_reason = Some(reason);
        Ok(())
    }

    /// Advance time (`moq_session_tick`), for when
    /// [`next_deadline`](Session::next_deadline) has come.
    ///
    /// # Errors
    /// The shared input results (closed, binding missing).
    pub fn tick(&mut self, now: Instant) -> Result<Tick> {
        // SAFETY: an advancing call on a live session.
        match unsafe { sys::moq_session_tick(self.ptr(), now.as_micros()) } {
            sys::MOQ_OK => Ok(Tick::Done),
            sys::MOQ_ERR_WOULD_BLOCK => Ok(Tick::TerminalOwed),
            code => Err(Error::from_code(code)),
        }
    }

    /// The next internal deadline; `None` when there is none.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        // SAFETY: an observing call on a live session.
        let due = unsafe { sys::moq_session_next_deadline_us(self.ptr()) };
        (due != u64::MAX).then(|| Instant::from_micros(due))
    }

    /// Whether the session holds outbound work it has not handed over: an
    /// unpolled action, or retained work it will emit later.
    #[must_use]
    pub fn has_pending_output(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_has_pending_output(self.ptr()) }
    }

    /// Action slots available now.
    #[must_use]
    pub fn action_capacity(&self) -> usize {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_action_capacity(self.ptr()) }
    }

    /// The draft lets a subscription carry a fill (draft 22).
    #[must_use]
    pub fn supports_fill(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_supports_fill(self.ptr()) }
    }

    /// The draft permits the zero-field root namespace (drafts 18 and 22).
    #[must_use]
    pub fn supports_root_namespace(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_supports_zero_field_track_namespace(self.ptr()) }
    }

    // -- Inputs -------------------------------------------------------------

    /// Map an input's result to its custody.
    fn custody<T>(
        &self,
        rc: sys::moq_result_t,
        stream: Option<StreamRef>,
        input: T,
        fin: bool,
    ) -> std::result::Result<Fed, FeedError<T>> {
        let refused = |reason| {
            Err(FeedError::NotAdmitted(NotAdmitted {
                input,
                fin,
                reason,
                error: Error::from_code(rc),
            }))
        };
        match rc {
            sys::MOQ_OK => Ok(Fed::Accepted),
            sys::MOQ_ERR_WOULD_BLOCK => match stream {
                // The session owns the stream: it kept what it needs.
                Some(stream) if self.has_transport_stream(stream) => Ok(Fed::Pending),
                Some(_) => refused(NotAdmittedReason::NoEntry),
                // Control bytes: always retained on WOULD_BLOCK.
                None => Ok(Fed::Pending),
            },
            sys::MOQ_ERR_BUFFER => refused(NotAdmittedReason::NoRoom),
            // "Closed, or its setup has not completed": nothing consumed.
            sys::MOQ_ERR_CLOSED if stream.is_some() && !self.is_closed() => {
                refused(NotAdmittedReason::SetupIncomplete)
            }
            code => Err(FeedError::Failed(Error::from_code(code))),
        }
    }

    /// Feed bytes of the control channel (`moq_session_on_control_bytes`),
    /// borrowed for the call.
    ///
    /// `Ok(Fed::Pending)` when the session took the bytes but a message waits
    /// for output to drain (the action or event queue is full, or the send
    /// buffer holds earlier output): drain, then
    /// [`process_pending`](Session::process_pending); the bytes are not
    /// delivered again.
    ///
    /// # Errors
    /// [`FeedError::NotAdmitted`] with [`NotAdmittedReason::NoRoom`] when the
    /// bytes do not fit the control buffer now; nothing was copied, and they
    /// come back to be delivered again. [`FeedError::Failed`] otherwise, with
    /// the bytes taken: [`Error::LIMIT`] when what handling a message must
    /// send can never fit this side's own output capacity (its send buffer
    /// or output scratch; for example a draft-16 server whose setup does not
    /// fit, see [`SessionConfig::setup_auth_token`]), which no retry
    /// changes; after another failure other than the shared input results
    /// the bytes may be buffered too. Bytes taken are not delivered again.
    pub fn on_control_bytes<'input>(
        &mut self,
        bytes: &'input [u8],
        now: Instant,
    ) -> std::result::Result<Fed, FeedError<&'input [u8]>> {
        // SAFETY: an advancing call; the bytes are borrowed for it.
        let rc = unsafe {
            sys::moq_session_on_control_bytes(
                self.ptr(),
                data_ptr(bytes),
                bytes.len(),
                now.as_micros(),
            )
        };
        self.custody(rc, None, bytes, false)
    }

    /// Handle control input the session kept (`moq_session_process_pending`).
    /// `Ok(Fed::Pending)` while a message still waits for output to drain.
    ///
    /// # Errors
    /// [`Error::LIMIT`] while a buffered message can never be handled (see
    /// [`on_control_bytes`](Session::on_control_bytes)), on every retry; the
    /// native code of another failure.
    pub fn process_pending(&mut self, now: Instant) -> Result<Fed> {
        // SAFETY: an advancing call on a live session.
        match unsafe { sys::moq_session_process_pending(self.ptr(), now.as_micros()) } {
            sys::MOQ_OK => Ok(Fed::Accepted),
            sys::MOQ_ERR_WOULD_BLOCK => Ok(Fed::Pending),
            code => Err(Error::from_code(code)),
        }
    }

    /// Feed bytes of a peer data stream (`moq_session_on_data_bytes`),
    /// borrowed for the call. `fin` ends the stream after them.
    /// `Ok(Fed::Pending)` when the session owns the stream and kept what it
    /// needs (or is stopping it): drain, then
    /// [`resume_stream`](Session::resume_stream); the bytes are not
    /// delivered again.
    ///
    /// # Errors
    /// [`FeedError::NotAdmitted`]: a new stream with no free receive entry,
    /// or setup not complete; the session kept nothing, and the host keeps
    /// these exact bytes and `fin` and delivers them again. No data delivery
    /// is refused for room ([`NotAdmittedReason::NoRoom`] does not occur).
    /// [`FeedError::Failed`] with the bytes taken: [`Error::LIMIT`] when
    /// what handling them must send can never fit this side's own capacity
    /// (a draft-16 fetch cancellation larger than the send buffer); the
    /// session is closed; or a failure whose retention C does not state
    /// (`NOMEM`). The host ends the session.
    pub fn on_data_bytes<'input>(
        &mut self,
        stream: StreamRef,
        bytes: &'input [u8],
        fin: bool,
        now: Instant,
    ) -> std::result::Result<Fed, FeedError<&'input [u8]>> {
        // SAFETY: an advancing call; the bytes are borrowed for it.
        let rc = unsafe {
            sys::moq_session_on_data_bytes(
                self.ptr(),
                stream.raw(),
                data_ptr(bytes),
                bytes.len(),
                fin,
                now.as_micros(),
            )
        };
        self.custody(rc, Some(stream), bytes, fin)
    }

    /// Feed an owned buffer of a peer data stream (`moq_session_on_data_rcbuf`).
    /// The buffer is consumed: when the session keeps data it adds its own
    /// reference or copies, and the binding releases the caller's.
    ///
    /// # Errors
    /// [`FeedError::NotAdmitted`] returns the same buffer, still exclusively
    /// the caller's (C consumed neither bytes nor FIN), for delivery again.
    /// [`FeedError::Failed`] releases it, as for
    /// [`on_data_bytes`](Session::on_data_bytes), [`Error::LIMIT`] included.
    pub fn on_data(
        &mut self,
        stream: StreamRef,
        data: Buffer,
        fin: bool,
        now: Instant,
    ) -> std::result::Result<Fed, FeedError<Buffer>> {
        // SAFETY: an advancing call; the buffer is a live reference this
        // owner holds, which C never releases.
        let rc = unsafe {
            sys::moq_session_on_data_rcbuf(
                self.ptr(),
                stream.raw(),
                data.as_ptr(),
                fin,
                now.as_micros(),
            )
        };
        match self.custody(rc, Some(stream), data, fin) {
            Err(FeedError::NotAdmitted(refused)) => {
                // C states nothing was retained. A buffer still carrying
                // another reference would not be exclusive any more; it is
                // released rather than returned.
                // SAFETY: a live rcbuf the refused record holds.
                if unsafe { sys::moq_rcbuf_refcount(refused.input.as_ptr()) } == 1 {
                    Err(FeedError::NotAdmitted(refused))
                } else {
                    Err(FeedError::Failed(Error::INTERNAL))
                }
            }
            // Accepted, pending or failed: the session holds its own
            // reference where it kept data; the caller's is released here.
            other => other,
        }
    }

    /// Resume a data stream the session kept input for: the empty delivery
    /// (`moq_session_on_data_bytes` with no bytes). On a ref the session does
    /// not know it does nothing.
    ///
    /// # Errors
    /// The native code of a failure.
    pub fn resume_stream(&mut self, stream: StreamRef, now: Instant) -> Result<Fed> {
        // SAFETY: an advancing call with no bytes.
        let rc = unsafe {
            sys::moq_session_on_data_bytes(
                self.ptr(),
                stream.raw(),
                ptr::null(),
                0,
                false,
                now.as_micros(),
            )
        };
        match rc {
            sys::MOQ_OK => Ok(Fed::Accepted),
            sys::MOQ_ERR_WOULD_BLOCK => Ok(Fed::Pending),
            code => Err(Error::from_code(code)),
        }
    }

    /// Whether the session could admit a new peer data stream now. Advisory,
    /// not a reservation; true once the session is no longer active, so
    /// input still reaches its terminal path.
    #[must_use]
    pub fn can_admit_data_stream(&self) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_can_admit_data_stream(self.ptr()) }
    }

    /// Whether the session holds `stream`: receive state for a peer data
    /// stream (one it stopped included) or a request stream.
    #[must_use]
    pub fn has_transport_stream(&self, stream: StreamRef) -> bool {
        // SAFETY: an observing call on a live session.
        unsafe { sys::moq_session_has_transport_stream(self.ptr(), stream.raw()) }
    }

    fn signal(
        rc: sys::moq_result_t,
        signal: PeerSignal,
        stream: StreamRef,
        error_code: u64,
    ) -> std::result::Result<(), SignalError> {
        match rc {
            sys::MOQ_OK => Ok(()),
            sys::MOQ_ERR_WOULD_BLOCK => Err(SignalError::Owed(OwedSignal {
                signal,
                stream,
                error_code,
            })),
            code => Err(SignalError::Failed(Error::from_code(code))),
        }
    }

    /// The peer reset a data stream (`moq_session_on_data_reset`).
    ///
    /// # Errors
    /// [`SignalError::Owed`]: the reset is recorded but its visible closure
    /// could not be queued; deliver it again
    /// ([`redeliver`](Session::redeliver)) after draining.
    pub fn on_data_reset(
        &mut self,
        stream: StreamRef,
        error_code: u64,
        now: Instant,
    ) -> std::result::Result<(), SignalError> {
        // SAFETY: an advancing call on a live session.
        let rc = unsafe {
            sys::moq_session_on_data_reset(self.ptr(), stream.raw(), error_code, now.as_micros())
        };
        Session::signal(rc, PeerSignal::DataReset, stream, error_code)
    }

    /// The peer sent STOP_SENDING on a data stream this side opened
    /// (`moq_session_on_data_stop_ex`). A stop is not a reset: it is never
    /// delivered through [`on_data_reset`](Session::on_data_reset).
    ///
    /// `Ok` also records the STOP for the reset of the stream the session
    /// still holds, whatever its first cause, so that reset may be claimed
    /// [`PrefixFact::Waived`]; its code, cause and terminal event stay as
    /// they are. Only `Ok` records it for that claim.
    ///
    /// # Errors
    /// [`SignalError::Owed`]: output capacity is missing. A fill stream's
    /// stop is already recorded with its first cause, and its reset stays
    /// owed until an action slot frees: delivering it again is safe, not
    /// required. A FETCH teardown that does not fit yet changed nothing:
    /// deliver it again after draining.
    /// [`SignalError::Failed`]: [`Error::INVAL`] for a code above `2^62-1`,
    /// [`Error::CLOSED`].
    pub fn on_data_stop(
        &mut self,
        stream: StreamRef,
        error_code: u64,
        now: Instant,
    ) -> std::result::Result<DataEnd, SignalError> {
        // SAFETY: zero is a valid bit pattern for this scalar record.
        let mut out: sys::moq_data_end_result_t = unsafe { mem::zeroed() };
        // SAFETY: an advancing call; the result record is written within the
        // size passed.
        let rc = unsafe {
            sys::moq_session_on_data_stop_ex(
                self.ptr(),
                stream.raw(),
                error_code,
                now.as_micros(),
                &mut out,
                mem::size_of::<sys::moq_data_end_result_t>(),
            )
        };
        Session::signal(rc, PeerSignal::DataStop, stream, error_code)?;
        DataEnd::from_raw(&out).map_err(SignalError::Failed)
    }

    /// The host holds bytes of a subgroup's data stream past their tag
    /// deadline (`moq_session_on_held_data_expired`).
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] for a FETCH or fill stream (no deadline);
    /// [`Error::CLOSED`].
    pub fn on_held_data_expired(&mut self, stream: StreamRef, now: Instant) -> Result<DataEnd> {
        // SAFETY: zero is a valid bit pattern for this scalar record.
        let mut out: sys::moq_data_end_result_t = unsafe { mem::zeroed() };
        // SAFETY: an advancing call; the record is written within its size.
        raw::check(unsafe {
            sys::moq_session_on_held_data_expired(
                self.ptr(),
                stream.raw(),
                now.as_micros(),
                &mut out,
                mem::size_of::<sys::moq_data_end_result_t>(),
            )
        })?;
        DataEnd::from_raw(&out)
    }

    /// A data stream this side opened reached "all data committed": every
    /// byte through its FIN was acknowledged (`moq_session_on_data_committed`).
    ///
    /// # Errors
    /// [`Error::CLOSED`], [`Error::WRONG_STATE`] while a required binding is
    /// missing.
    pub fn on_data_committed(&mut self, stream: StreamRef, now: Instant) -> Result<()> {
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_on_data_committed(self.ptr(), stream.raw(), now.as_micros())
        })
    }

    /// Feed bytes of the peer's half of a bidirectional stream
    /// (`moq_session_on_bidi_stream_bytes`), borrowed for the call.
    /// `Ok(Fed::Pending)` when the session owns the stream and kept the
    /// bytes, a message waiting for capacity (an event, an action, a drain
    /// reference, or room earlier output holds): drain, then
    /// [`resume_bidi`](Session::resume_bidi); the bytes are not delivered
    /// again.
    ///
    /// # Errors
    /// [`FeedError::NotAdmitted`]: a new draft-16 namespace subscription
    /// with no free entry, bytes that do not fit the request's buffer, or a
    /// draft-16 setup not complete; nothing retained, deliver again.
    /// [`FeedError::Failed`] otherwise. [`Error::LIMIT`] means the session
    /// took the bytes but handling a message needs output that can never fit
    /// this side's own capacity (send buffer or output scratch); they are not
    /// delivered again.
    pub fn on_bidi_bytes<'input>(
        &mut self,
        stream: StreamRef,
        bytes: &'input [u8],
        fin: bool,
        now: Instant,
    ) -> std::result::Result<Fed, FeedError<&'input [u8]>> {
        // SAFETY: an advancing call; the bytes are borrowed for it.
        let rc = unsafe {
            sys::moq_session_on_bidi_stream_bytes(
                self.ptr(),
                stream.raw(),
                data_ptr(bytes),
                bytes.len(),
                fin,
                now.as_micros(),
            )
        };
        if rc == sys::MOQ_ERR_WRONG_STATE && !self.binding_missing() {
            // Draft 16: setup not complete; nothing consumed.
            return Err(FeedError::NotAdmitted(NotAdmitted {
                input: bytes,
                fin,
                reason: NotAdmittedReason::SetupIncomplete,
                error: Error::WRONG_STATE,
            }));
        }
        self.custody(rc, Some(stream), bytes, fin)
    }

    /// Resume a bidirectional stream the session kept input for: the empty
    /// delivery.
    ///
    /// # Errors
    /// The native code of a failure.
    pub fn resume_bidi(&mut self, stream: StreamRef, now: Instant) -> Result<Fed> {
        // SAFETY: an advancing call with no bytes.
        let rc = unsafe {
            sys::moq_session_on_bidi_stream_bytes(
                self.ptr(),
                stream.raw(),
                ptr::null(),
                0,
                false,
                now.as_micros(),
            )
        };
        match rc {
            sys::MOQ_OK => Ok(Fed::Accepted),
            sys::MOQ_ERR_WOULD_BLOCK => Ok(Fed::Pending),
            code => Err(Error::from_code(code)),
        }
    }

    /// The peer reset its half of a bidirectional stream
    /// (`moq_session_on_bidi_stream_reset`).
    ///
    /// # Errors
    /// [`SignalError::Owed`]: deliver it again after draining; partial
    /// effects are not repeated.
    pub fn on_bidi_reset(
        &mut self,
        stream: StreamRef,
        error_code: u64,
        now: Instant,
    ) -> std::result::Result<(), SignalError> {
        // SAFETY: an advancing call on a live session.
        let rc = unsafe {
            sys::moq_session_on_bidi_stream_reset(
                self.ptr(),
                stream.raw(),
                error_code,
                now.as_micros(),
            )
        };
        Session::signal(rc, PeerSignal::BidiReset, stream, error_code)
    }

    /// The peer sent STOP_SENDING on a bidirectional stream
    /// (`moq_session_on_bidi_stream_stop`).
    ///
    /// # Errors
    /// As for [`on_bidi_reset`](Session::on_bidi_reset).
    pub fn on_bidi_stop(
        &mut self,
        stream: StreamRef,
        error_code: u64,
        now: Instant,
    ) -> std::result::Result<(), SignalError> {
        // SAFETY: an advancing call on a live session.
        let rc = unsafe {
            sys::moq_session_on_bidi_stream_stop(
                self.ptr(),
                stream.raw(),
                error_code,
                now.as_micros(),
            )
        };
        Session::signal(rc, PeerSignal::BidiStop, stream, error_code)
    }

    /// Deliver an owed signal again, with its original arguments. A data
    /// stop's outcome comes back with it.
    ///
    /// # Errors
    /// As for the original call.
    pub fn redeliver(
        &mut self,
        owed: OwedSignal,
        now: Instant,
    ) -> std::result::Result<Option<DataEnd>, SignalError> {
        let OwedSignal {
            signal,
            stream,
            error_code,
        } = owed;
        match signal {
            PeerSignal::DataReset => self.on_data_reset(stream, error_code, now).map(|()| None),
            PeerSignal::DataStop => self.on_data_stop(stream, error_code, now).map(Some),
            PeerSignal::BidiReset => self.on_bidi_reset(stream, error_code, now).map(|()| None),
            PeerSignal::BidiStop => self.on_bidi_stop(stream, error_code, now).map(|()| None),
        }
    }

    /// Feed a received datagram (`moq_session_on_datagram`), borrowed for
    /// the call; the session copies what it keeps.
    ///
    /// # Errors
    /// [`FeedError::NotAdmitted`] ([`NotAdmittedReason::DatagramRefused`] or
    /// setup not complete): nothing retained; dropping it is correct.
    /// [`FeedError::Failed`] otherwise.
    pub fn on_datagram<'input>(
        &mut self,
        bytes: &'input [u8],
        now: Instant,
    ) -> std::result::Result<(), FeedError<&'input [u8]>> {
        // SAFETY: an advancing call; the bytes are borrowed for it.
        let rc = unsafe {
            sys::moq_session_on_datagram(self.ptr(), data_ptr(bytes), bytes.len(), now.as_micros())
        };
        let reason = match rc {
            sys::MOQ_OK => return Ok(()),
            sys::MOQ_ERR_WOULD_BLOCK => NotAdmittedReason::DatagramRefused,
            sys::MOQ_ERR_WRONG_STATE if !self.binding_missing() => {
                NotAdmittedReason::SetupIncomplete
            }
            code => return Err(FeedError::Failed(Error::from_code(code))),
        };
        Err(FeedError::NotAdmitted(NotAdmitted {
            input: bytes,
            fin: false,
            reason,
            error: Error::from_code(rc),
        }))
    }

    /// Where a peer-opened unidirectional stream belongs, from its leading
    /// bytes (`moq_session_route_peer_uni`). Observing: nothing is consumed,
    /// and the caller still feeds every byte to the route's input.
    ///
    /// # Errors
    /// [`Error::CLOSED`]: no stream is routed. [`Error::INTERNAL`]: the
    /// profile gave a classification C cannot map.
    pub fn route_peer_uni(&self, leading: &[u8]) -> Result<UniRoute> {
        let mut route = sys::MOQ_UNI_ROUTE_NONE;
        // SAFETY: an observing call; the bytes are borrowed for it.
        raw::check(unsafe {
            sys::moq_session_route_peer_uni(
                self.ptr(),
                data_ptr(leading),
                leading.len(),
                &mut route,
            )
        })?;
        UniRoute::from_raw(route)
    }

    fn retirement(&self, rc: sys::moq_result_t) -> Result<Retirement> {
        match rc {
            sys::MOQ_OK => Ok(Retirement::Released),
            sys::MOQ_ERR_STALE_HANDLE => Ok(Retirement::NotHeld),
            sys::MOQ_ERR_WRONG_STATE if !self.binding_missing() => Ok(Retirement::NotYet),
            code => Err(Error::from_code(code)),
        }
    }

    /// The transport retired a peer data stream this session stopped, and
    /// its FIN or RESET will never be delivered
    /// (`moq_session_on_data_stream_retired`).
    ///
    /// The host must have established that it polled the stream's
    /// `STOP_DATA` and the transport accepted it, that the transport reported
    /// it will deliver nothing more on the stream (a FIN or RESET is not
    /// that report: feed those), and that it holds no unfed bytes or FIN of
    /// it. The session cannot check the last two.
    ///
    /// # Errors
    /// [`Error::CLOSED`]; [`Error::WRONG_STATE`] while a required binding is
    /// missing.
    pub fn on_data_stream_retired(
        &mut self,
        stream: StreamRef,
        now: Instant,
    ) -> Result<Retirement> {
        // SAFETY: an advancing call once admitted.
        let rc = unsafe {
            sys::moq_session_on_data_stream_retired(self.ptr(), stream.raw(), now.as_micros())
        };
        self.retirement(rc)
    }

    /// The transport retired a request stream this session aborted, and the
    /// peer's FIN or RESET will never be delivered
    /// (`moq_session_on_bidi_stream_retired`). The host's obligations are
    /// those of [`on_data_stream_retired`](Session::on_data_stream_retired),
    /// with the abandoning action (`ABORT_BIDI_STREAM` or `STOP_BIDI_STREAM`)
    /// and no unwritten output of the stream either.
    ///
    /// # Errors
    /// As for [`on_data_stream_retired`](Session::on_data_stream_retired).
    pub fn on_bidi_stream_retired(
        &mut self,
        stream: StreamRef,
        now: Instant,
    ) -> Result<Retirement> {
        // SAFETY: an advancing call once admitted.
        let rc = unsafe {
            sys::moq_session_on_bidi_stream_retired(self.ptr(), stream.raw(), now.as_micros())
        };
        self.retirement(rc)
    }

    /// A stream of the control channel of a live session was ended at the
    /// transport by the peer: finished, reset, or told to stop
    /// (`moq_session_on_control_stream_ended`). The session closes with a
    /// protocol violation and queues its close action and event. A stream
    /// notification caused by the connection going away is reported through
    /// [`on_transport_close`](Session::on_transport_close) instead.
    ///
    /// # Errors
    /// None in practice; the native code otherwise.
    pub fn on_control_stream_ended(&mut self, now: Instant) -> Result<()> {
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe { sys::moq_session_on_control_stream_ended(self.ptr(), now.as_micros()) })
    }

    /// The transport closed (`moq_session_on_transport_close`): the session
    /// closes with one close event and no close action; everything queued
    /// is released.
    ///
    /// # Errors
    /// None in practice; the native code otherwise.
    pub fn on_transport_close(&mut self, code: u64, now: Instant) -> Result<()> {
        // SAFETY: an advancing call on a live session.
        raw::check(unsafe {
            sys::moq_session_on_transport_close(self.ptr(), code, now.as_micros())
        })
    }

    // -- Outputs ------------------------------------------------------------

    /// Take the next action (`moq_session_poll_actions_tagged` in the
    /// tagged mode, else `moq_session_poll_actions_ex`). Observing in C;
    /// the action borrows the owner until it is dropped or converted.
    ///
    /// # Errors
    /// The native code of a failed poll; [`Error::ABI_MISMATCH`] for a
    /// record outside its size contract, which is released.
    pub fn poll_action(&mut self) -> Result<ActionPoll<'_>> {
        // SAFETY: zero is a valid bit pattern for these plain C records.
        let mut action: sys::moq_action_t = unsafe { mem::zeroed() };
        // SAFETY: as above.
        let mut tag: sys::moq_action_tag_t = unsafe { mem::zeroed() };
        let mut count = 0usize;
        let tagged = self.tagged_output();
        // SAFETY: one element of each record, written within the sizes
        // passed; the session is live.
        let rc = unsafe {
            if tagged {
                sys::moq_session_poll_actions_tagged(
                    self.ptr(),
                    (&mut action as *mut sys::moq_action_t).cast::<c_void>(),
                    mem::size_of::<sys::moq_action_t>(),
                    (&mut tag as *mut sys::moq_action_tag_t).cast::<c_void>(),
                    mem::size_of::<sys::moq_action_tag_t>(),
                    1,
                    &mut count,
                )
            } else {
                sys::moq_session_poll_actions_ex(
                    self.ptr(),
                    (&mut action as *mut sys::moq_action_t).cast::<c_void>(),
                    1,
                    mem::size_of::<sys::moq_action_t>(),
                    &mut count,
                )
            }
        };
        match rc {
            sys::MOQ_OK if count == 0 => Ok(ActionPoll::Empty),
            // SAFETY: a record the poll wrote and transferred; the returned
            // action borrows `self` exclusively for its lifetime.
            sys::MOQ_OK => Ok(ActionPoll::Ready(unsafe {
                Action::from_raw(action, tagged.then_some(&tag))
            }?)),
            sys::MOQ_ERR_WOULD_BLOCK => Ok(ActionPoll::Waiting),
            code => Err(Error::from_code(code)),
        }
    }

    /// Take the next event (`moq_session_poll_events_ex`). `None` when no
    /// event is queued. The event borrows the owner until it is dropped.
    ///
    /// # Errors
    /// The native code of a failed poll; [`Error::ABI_MISMATCH`] for a
    /// record outside its size contract, which is consumed and released.
    pub fn poll_event(&mut self) -> Result<Option<Event<'_>>> {
        // SAFETY: zero is a valid bit pattern for this plain C record.
        let mut record = RawEvent(unsafe { mem::zeroed() });
        let mut count = 0usize;
        // SAFETY: one element written within the size passed.
        raw::check(unsafe {
            sys::moq_session_poll_events_ex(
                self.ptr(),
                (&mut record.0 as *mut sys::moq_event_t).cast::<c_void>(),
                1,
                mem::size_of::<sys::moq_event_t>(),
                &mut count,
            )
        })?;
        if count == 0 {
            return Ok(None);
        }
        // SAFETY: a record the poll wrote and transferred; the event borrows
        // `self` exclusively for its lifetime; `record` releases whatever
        // the decoder did not adopt.
        let event = unsafe { event::decode(&mut record.0, self.owner) }?;
        Ok(Some(event))
    }

    /// Offer the next data reset the host may take, or the claim it holds
    /// (`moq_session_offer_data_reset`, tagged mode). No effect.
    ///
    /// # Errors
    /// [`Error::WRONG_STATE`] outside the tagged mode, [`Error::CLOSED`].
    pub fn offer_data_reset(&self) -> Result<ResetOfferPoll> {
        // SAFETY: zero is a valid bit pattern for this scalar record.
        let mut offer: sys::moq_data_reset_offer_t = unsafe { mem::zeroed() };
        let mut count = 0usize;
        // SAFETY: an observing call; the record is written within its size.
        let rc = unsafe {
            sys::moq_session_offer_data_reset(
                self.ptr(),
                &mut offer,
                mem::size_of::<sys::moq_data_reset_offer_t>(),
                &mut count,
            )
        };
        match rc {
            sys::MOQ_OK if count == 0 => Ok(ResetOfferPoll::Empty),
            sys::MOQ_OK => Ok(ResetOfferPoll::Offer(DataResetOffer::from_raw(offer)?)),
            sys::MOQ_ERR_WOULD_BLOCK => Ok(ResetOfferPoll::Waiting),
            code => Err(Error::from_code(code)),
        }
    }

    /// Claim an offered reset with the host's fact about the stream's
    /// prefix (`moq_session_claim_data_reset`). The stream's queued data
    /// leaves the session; the reset stays the session's until
    /// [`commit_data_reset`](Session::commit_data_reset), unless the claim
    /// is settled (unopened).
    ///
    /// # Errors
    /// Refusals consume nothing: [`Error::STALE_HANDLE`] (no current offer
    /// with that identity), [`Error::WRONG_STATE`] (already claimed, or the
    /// prefix fact is not allowed: see [`PrefixFact`]), [`Error::WOULD_BLOCK`]
    /// (an owed terminal event has no capacity), [`Error::CLOSED`]. A claim
    /// already made is not changed by a later STOP.
    pub fn claim_data_reset(
        &mut self,
        offer: &DataResetOffer,
        prefix: PrefixFact,
    ) -> Result<DataResetClaim> {
        // SAFETY: zero is a valid bit pattern for this scalar record.
        let mut claim: sys::moq_data_reset_claim_t = unsafe { mem::zeroed() };
        // SAFETY: an advancing call; the offer is a record the session wrote,
        // the claim is written within its size.
        raw::check(unsafe {
            sys::moq_session_claim_data_reset(
                self.ptr(),
                offer.raw(),
                mem::size_of::<sys::moq_data_reset_offer_t>(),
                prefix.raw(),
                &mut claim,
                mem::size_of::<sys::moq_data_reset_claim_t>(),
            )
        })?;
        DataResetClaim::from_raw(&claim)
    }

    /// Commit the claimed reset after the transport accepted it
    /// (`moq_session_commit_data_reset`).
    ///
    /// # Errors
    /// [`Error::STALE_HANDLE`] for another identity or a duplicate commit
    /// (the claim is kept); [`Error::CLOSED`] once closed (closing drops the
    /// claim).
    pub fn commit_data_reset(&mut self, claimed: &DataResetOffer) -> Result<()> {
        // SAFETY: an advancing call; the offer is a record the session wrote.
        raw::check(unsafe {
            sys::moq_session_commit_data_reset(
                self.ptr(),
                claimed.raw(),
                mem::size_of::<sys::moq_data_reset_offer_t>(),
            )
        })
    }

    /// Report the open of a counted data stream, once per stream
    /// (`moq_session_report_data_open`, tagged mode).
    ///
    /// # Errors
    /// [`Error::STALE_HANDLE`] when no unresolved open of exactly
    /// `(stream, owner)` exists; [`Error::WRONG_STATE`] outside the tagged
    /// mode or before the opening action was polled.
    pub fn report_data_open(
        &mut self,
        stream: StreamRef,
        owner: OutputOwner,
        outcome: DataOpenOutcome,
    ) -> Result<()> {
        // SAFETY: a live session.
        raw::check(unsafe {
            sys::moq_session_report_data_open(self.ptr(), stream.raw(), owner.raw(), outcome.raw())
        })
    }

    /// Report that the transport accepted the FIN of a local data stream
    /// (`moq_session_report_data_fin`, tagged mode).
    ///
    /// Acceptance means the transport took the write carrying the FIN, and
    /// every earlier byte of the stream has left the host. A refused or
    /// partial write is not acceptance, and neither is polling the action:
    /// acknowledgement is reported separately
    /// ([`on_data_committed`](Session::on_data_committed)). Report once for
    /// every such FIN of a local data stream the session handed out, a
    /// subgroup, FETCH or fill response alike.
    ///
    /// A closed subgroup whose output with a delivery deadline was handed
    /// out keeps its identity while that output may still be held, so a
    /// held expiry ([`on_held_data_expired`](Session::on_held_data_expired))
    /// or a STOP still commits its one
    /// [`OutgoingSubgroupTerminated`](crate::Event::OutgoingSubgroupTerminated).
    /// This report ends that hold. A host that never reports keeps those
    /// subgroups, and their places in the subgroup pool, until a reset, the
    /// owner's cancellation or the session's close ends them.
    ///
    /// The session is borrowed for the call, and the call stays serialized
    /// with the session's other calls (`&mut self`). It does not advance the
    /// session: it takes no time, runs no pending work, invalidates no
    /// borrow, releases no buffer, frees no slot and queues no event or
    /// action. The next advancing call may reclaim the subgroup.
    ///
    /// # Errors
    /// In this order, each refusal changing nothing:
    /// [`Error::INVAL`] for a ref in the peer range;
    /// [`Error::WRONG_STATE`] outside the tagged mode;
    /// [`Error::CLOSED`] once the session is closed.
    /// Past those, a report a reset made obsolete is `Ok(())` and changes
    /// nothing: the stream's reset is queued, claimed or still owed after a
    /// recognized terminal, or the session no longer keeps the stream. Only a
    /// subgroup still open, streaming or closing whose FIN has not been
    /// handed out (queued, or not yet written) is [`Error::WRONG_STATE`].
    /// `Ok(())` also when the session holds no identity for the ref (a
    /// stream with no deadline, one already reported, a FETCH or fill
    /// stream); a ref is never reused within a session, so a duplicate or
    /// late report cannot affect another stream, and `Ok(())` does not
    /// validate a ref this session never handed out.
    pub fn report_data_fin(&mut self, stream: StreamRef) -> Result<()> {
        // SAFETY: a live session, borrowed exclusively for the call; the
        // native call reads only the ref, keeps no pointer and transfers no
        // ownership.
        raw::check(unsafe { sys::moq_session_report_data_fin(self.ptr(), stream.raw()) })
    }
}
