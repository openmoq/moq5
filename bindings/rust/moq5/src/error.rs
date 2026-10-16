use std::ffi::CStr;
use std::fmt;

use crate::sys;

/// A libmoq result code other than `MOQ_OK`.
///
/// Every fallible call returns the code libmoq reported, so a caller can
/// match on the documented outcomes ([`Error::WOULD_BLOCK`],
/// [`Error::INTERRUPTED`], [`Error::CLOSED`], ...) exactly as the C API
/// describes them.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Error(i32);

/// `Result` with [`Error`] as the default error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// Allocation failed.
    pub const NOMEM: Error = Error(sys::MOQ_ERR_NOMEM);
    /// Invalid argument or configuration.
    pub const INVAL: Error = Error(sys::MOQ_ERR_INVAL);
    /// Protocol or parse failure.
    pub const PROTO: Error = Error(sys::MOQ_ERR_PROTO);
    /// The object is terminal.
    pub const CLOSED: Error = Error(sys::MOQ_ERR_CLOSED);
    /// The call is not legal in the object's current state.
    pub const WRONG_STATE: Error = Error(sys::MOQ_ERR_WRONG_STATE);
    /// A handle refers to a freed entity.
    pub const STALE_HANDLE: Error = Error(sys::MOQ_ERR_STALE_HANDLE);
    /// A handle belongs to another session.
    pub const WRONG_SESSION: Error = Error(sys::MOQ_ERR_WRONG_SESSION);
    /// Retryable: a bounded queue is momentarily full.
    pub const WOULD_BLOCK: Error = Error(sys::MOQ_ERR_WOULD_BLOCK);
    /// A caller buffer was too small.
    pub const BUFFER: Error = Error(sys::MOQ_ERR_BUFFER);
    /// The peer's request limit blocks a new request.
    pub const REQUEST_BLOCKED: Error = Error(sys::MOQ_ERR_REQUEST_BLOCKED);
    /// A struct size is below the frozen ABI floor.
    pub const ABI_MISMATCH: Error = Error(sys::MOQ_ERR_ABI_MISMATCH);
    /// The peer sent GOAWAY.
    pub const GOAWAY: Error = Error(sys::MOQ_ERR_GOAWAY);
    /// The endpoint's interrupt latch is set (see
    /// [`Endpoint::set_interrupted`](crate::Endpoint::set_interrupted)). Not
    /// terminal: calls block normally again once the latch clears.
    pub const INTERRUPTED: Error = Error(sys::MOQ_ERR_INTERRUPTED);
    /// This build of libmoq cannot serve the request.
    pub const UNSUPPORTED: Error = Error(sys::MOQ_ERR_UNSUPPORTED);
    /// Internal error.
    pub const INTERNAL: Error = Error(sys::MOQ_ERR_INTERNAL);

    /// Wrap a libmoq result code. `code` should not be `MOQ_OK`.
    pub const fn from_code(code: i32) -> Error {
        Error(code)
    }

    /// The libmoq result code (`moq_result_t`).
    pub const fn code(self) -> i32 {
        self.0
    }

    fn name(self) -> Option<&'static str> {
        Some(match self {
            Error::NOMEM => "NOMEM",
            Error::INVAL => "INVAL",
            Error::PROTO => "PROTO",
            Error::CLOSED => "CLOSED",
            Error::WRONG_STATE => "WRONG_STATE",
            Error::STALE_HANDLE => "STALE_HANDLE",
            Error::WRONG_SESSION => "WRONG_SESSION",
            Error::WOULD_BLOCK => "WOULD_BLOCK",
            Error::BUFFER => "BUFFER",
            Error::REQUEST_BLOCKED => "REQUEST_BLOCKED",
            Error::ABI_MISMATCH => "ABI_MISMATCH",
            Error::GOAWAY => "GOAWAY",
            Error::INTERRUPTED => "INTERRUPTED",
            Error::UNSUPPORTED => "UNSUPPORTED",
            Error::INTERNAL => "INTERNAL",
            Error(sys::MOQ_DONE) => "DONE",
            _ => return None,
        })
    }
}

/// Map a libmoq result code: `MOQ_OK` is success, anything else an error.
pub(crate) fn check(rc: sys::moq_result_t) -> Result<()> {
    if rc == sys::MOQ_OK {
        Ok(())
    } else {
        Err(Error(rc))
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => write!(f, "Error({name}, {})", self.0),
            None => write!(f, "Error({})", self.0),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // SAFETY: moq_strerror returns a static string for any code.
        let text = unsafe { sys::moq_strerror(self.0) };
        if text.is_null() {
            write!(f, "libmoq error {}", self.0)
        } else {
            let text = unsafe { CStr::from_ptr(text) }.to_string_lossy();
            write!(f, "{text} ({})", self.0)
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_round_trip() {
        assert_eq!(Error::from_code(-2), Error::INVAL);
        assert_eq!(Error::WOULD_BLOCK.code(), sys::MOQ_ERR_WOULD_BLOCK);
        assert_eq!(check(sys::MOQ_OK), Ok(()));
        assert_eq!(check(sys::MOQ_ERR_CLOSED), Err(Error::CLOSED));
        assert_eq!(
            format!("{:?}", Error::INTERRUPTED),
            "Error(INTERRUPTED, -13)"
        );
        assert_eq!(format!("{:?}", Error::from_code(-1000)), "Error(-1000)");
        assert!(Error::CLOSED.to_string().ends_with("(-4)"));
    }
}
