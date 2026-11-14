//! Who holds an input after the session answered it.

use std::fmt;

use super::stream::StreamRef;
use crate::{Buffer, Error};

/// What the session did with an input it took.
///
/// Closed: the two outcomes of an admitted input in the native contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[must_use]
pub enum Fed {
    /// Consumed. Nothing to retry.
    Accepted,
    /// Retained by the session: it could not finish handling the input
    /// until outputs are drained. Drain actions and events, then resume
    /// (`process_pending` for control, `resume_stream` for a data stream,
    /// `resume_bidi` for a bidirectional one). Do not deliver the same
    /// bytes again.
    Pending,
}

/// Why an input was not admitted.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NotAdmittedReason {
    /// A new stream found no free entry (data: no receive entry; draft-16
    /// namespace subscription: no free entry). Offer it again once
    /// [`Session::can_admit_data_stream`](crate::Session::can_admit_data_stream)
    /// is true (data) or an entry is released.
    NoEntry,
    /// The bytes do not fit the session's buffer now
    /// (`MOQ_ERR_BUFFER`). Room returns only as buffered input is handled;
    /// a host that cannot show that will happen ends the session for its
    /// own resource limit.
    NoRoom,
    /// The session's setup has not completed. Offer it again once it has.
    /// The native code says which input refused it: `MOQ_ERR_CLOSED` from
    /// a data stream on a session that is not closed, `MOQ_ERR_WRONG_STATE`
    /// from a draft-16 bidirectional stream or a datagram.
    SetupIncomplete,
    /// A datagram refused for pressure, or permanently for its size: the
    /// result does not say which. Dropping it is always correct; a host that
    /// offers it again bounds its attempts.
    DatagramRefused,
}

/// Nothing of the input was retained: the host still holds it and delivers
/// the same input, with the same `fin`, again later. The record is the whole
/// answer: the input to deliver again, how, why, and the native code that
/// said so. It is not `#[non_exhaustive]`, so a host can take it apart by
/// value and keep the input; a field added later is a breaking change, made
/// deliberately with the rest of the answer.
#[derive(Debug, PartialEq, Eq)]
#[must_use = "the input must be delivered again"]
pub struct NotAdmitted<T> {
    /// The input, untouched: the borrowed bytes the host passed, or the
    /// owned [`Buffer`], still exclusively the host's.
    pub input: T,
    /// The `fin` that went with it.
    pub fin: bool,
    /// Why it was refused: what the host waits for before delivering it
    /// again. Independent of the native code.
    pub reason: NotAdmittedReason,
    /// The native code the call returned, as returned.
    pub error: Error,
}

/// A refused feed.
///
/// Closed: a refused feed is either not admitted or failed; the whole answer.
#[derive(Debug, PartialEq, Eq)]
#[must_use]
pub enum FeedError<T> {
    /// Not admitted; nothing retained. Deliver it again.
    NotAdmitted(NotAdmitted<T>),
    /// The call failed. Whether anything was retained is not stated, so the
    /// input cannot be redelivered or resumed: a host that is not already
    /// closed ends the session. An owned input was released.
    Failed(Error),
}

impl<T> FeedError<T> {
    /// The native code the call returned, for an input not admitted as for
    /// a failure.
    pub fn error(&self) -> Error {
        match self {
            FeedError::NotAdmitted(refused) => refused.error,
            FeedError::Failed(error) => *error,
        }
    }
}

impl<T> fmt::Display for FeedError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FeedError::NotAdmitted(refused) => {
                write!(f, "input not admitted ({:?})", refused.reason)
            }
            FeedError::Failed(error) => write!(f, "input failed: {error}"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for FeedError<T> {}

/// Converting drops a refused input deliberately, keeping only the code.
impl<T> From<FeedError<T>> for Error {
    fn from(error: FeedError<T>) -> Error {
        error.error()
    }
}

/// Which peer signal an [`OwedSignal`] is.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PeerSignal {
    /// The peer reset a data stream.
    DataReset,
    /// The peer sent STOP_SENDING on a data stream this side opened.
    DataStop,
    /// The peer reset a bidirectional stream.
    BidiReset,
    /// The peer sent STOP_SENDING on a bidirectional stream.
    BidiStop,
}

/// A peer signal the session could not finish taking: it needs output
/// capacity first. Drain actions and events, then deliver it again with
/// [`Session::redeliver`](crate::Session::redeliver), which repeats the same
/// call with these same arguments. What the first delivery recorded (its
/// cause, partial effects) is kept, not repeated.
///
/// Closed: the signal and the arguments to deliver it again with are the whole answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[must_use = "the signal must be delivered again"]
pub struct OwedSignal {
    /// The signal.
    pub signal: PeerSignal,
    /// The stream.
    pub stream: StreamRef,
    /// The peer's code, as first delivered.
    pub error_code: u64,
}

/// A refused peer signal.
///
/// Closed: a refused signal is either owed or failed; the whole answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub enum SignalError {
    /// Owed: deliver it again after draining.
    Owed(OwedSignal),
    /// The call failed (for instance a closed session); nothing is owed.
    Failed(Error),
}

impl SignalError {
    /// The native code: `MOQ_ERR_WOULD_BLOCK` for an owed signal.
    pub fn error(self) -> Error {
        match self {
            SignalError::Owed(_) => Error::WOULD_BLOCK,
            SignalError::Failed(error) => error,
        }
    }
}

impl fmt::Display for SignalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignalError::Owed(owed) => write!(f, "{:?} owed on {:?}", owed.signal, owed.stream),
            SignalError::Failed(error) => write!(f, "signal failed: {error}"),
        }
    }
}

impl std::error::Error for SignalError {}

/// Converting drops the obligation deliberately, keeping only the code.
impl From<SignalError> for Error {
    fn from(error: SignalError) -> Error {
        error.error()
    }
}

/// A refused write of owned buffers.
///
/// Closed: the error and the buffers it hands back are the whole answer.
#[derive(Debug)]
#[must_use]
pub struct WriteError {
    /// Why the write was refused.
    pub error: Error,
    /// The payload, when the call states that this refusal retains no
    /// reference to it and it is still exclusively the caller's: the very
    /// buffer, intact, for a retry. `None` otherwise: the binding released
    /// its reference.
    pub payload: Option<Buffer>,
    /// The object properties, on the same terms.
    pub properties: Option<Buffer>,
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "write refused: {}", self.error)
    }
}

impl std::error::Error for WriteError {}

/// Converting drops a returned payload deliberately, keeping only the code.
impl From<WriteError> for Error {
    fn from(error: WriteError) -> Error {
        error.error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_keep_their_codes() {
        let refused: FeedError<&[u8]> = FeedError::NotAdmitted(NotAdmitted {
            input: b"x",
            fin: true,
            reason: NotAdmittedReason::NoEntry,
            error: Error::WOULD_BLOCK,
        });
        assert_eq!(refused.error(), Error::WOULD_BLOCK);
        assert_eq!(Error::from(refused), Error::WOULD_BLOCK);
        // The reason does not decide the code: one reason, two codes.
        for code in [Error::CLOSED, Error::WRONG_STATE] {
            let setup: FeedError<&[u8]> = FeedError::NotAdmitted(NotAdmitted {
                input: b"y",
                fin: false,
                reason: NotAdmittedReason::SetupIncomplete,
                error: code,
            });
            assert_eq!(setup.error(), code);
            assert_eq!(Error::from(setup), code);
        }
        let failed: FeedError<&[u8]> = FeedError::Failed(Error::NOMEM);
        assert_eq!(Error::from(failed), Error::NOMEM);
        let owed = SignalError::Owed(OwedSignal {
            signal: PeerSignal::DataReset,
            stream: StreamRef::peer(0).expect("a peer ref"),
            error_code: 3,
        });
        assert_eq!(Error::from(owed), Error::WOULD_BLOCK);
        assert_eq!(
            Error::from(SignalError::Failed(Error::CLOSED)),
            Error::CLOSED
        );
    }
}
