use std::ffi::CStr;
use std::fmt;

use crate::sys;

/// A libmoq result code other than `MOQ_OK`.
///
/// Every fallible call returns the code the library reported, so a caller
/// can match on the documented outcomes ([`Error::WOULD_BLOCK`],
/// [`Error::INTERRUPTED`], [`Error::CLOSED`], ...) exactly as the C header
/// describes them. Every native code has a named constant; a code this crate
/// does not know is carried unchanged.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[must_use]
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
    /// A sized record is outside its size contract. The library reports it
    /// for an input or output below the frozen v0 floor; this crate reports
    /// it when the `struct_size` the library stamped on an output is below
    /// that floor or above the size the crate passed in. An owning record
    /// refused this way is released before the error returns.
    pub const ABI_MISMATCH: Error = Error(sys::MOQ_ERR_ABI_MISMATCH);
    /// The peer sent GOAWAY.
    pub const GOAWAY: Error = Error(sys::MOQ_ERR_GOAWAY);
    /// The endpoint's interrupt latch is set (see
    /// [`Endpoint::set_interrupted`](crate::Endpoint::set_interrupted)). Not
    /// terminal: calls block normally again once the latch clears.
    pub const INTERRUPTED: Error = Error(sys::MOQ_ERR_INTERRUPTED);
    /// This build of libmoq cannot serve the request.
    pub const UNSUPPORTED: Error = Error(sys::MOQ_ERR_UNSUPPORTED);
    /// Transport-bridge inbound data was not consumed; the adapter redelivers
    /// it. Never produced by the service tier this crate wraps.
    pub const INPUT_NOT_CONSUMED: Error = Error(sys::MOQ_ERR_INPUT_NOT_CONSUMED);
    /// A non-replenishable implementation limit was reached; retrying
    /// unchanged cannot succeed.
    pub const LIMIT: Error = Error(sys::MOQ_ERR_LIMIT);
    /// Configured admission policy refused the operation; draining alone need not help.
    pub const CAPACITY: Error = Error(sys::MOQ_ERR_CAPACITY);
    /// Internal error.
    pub const INTERNAL: Error = Error(sys::MOQ_ERR_INTERNAL);

    /// Wrap a libmoq result code. `code` should not be `MOQ_OK`.
    pub const fn from_code(code: i32) -> Error {
        Error(code)
    }

    /// The libmoq result code (`moq_result_t`).
    #[must_use]
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
            Error::INPUT_NOT_CONSUMED => "INPUT_NOT_CONSUMED",
            Error::LIMIT => "LIMIT",
            Error::CAPACITY => "CAPACITY",
            Error::INTERNAL => "INTERNAL",
            Error(sys::MOQ_DONE) => "DONE",
            _ => return None,
        })
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
        // SAFETY: moq_strerror returns a static NUL-terminated string, or
        // NULL, for any code.
        let text = unsafe { sys::moq_strerror(self.0) };
        if text.is_null() {
            write!(f, "libmoq error {}", self.0)
        } else {
            // SAFETY: non-NULL, static, NUL-terminated (above).
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
    fn every_native_code_is_named() {
        let named = [
            (Error::NOMEM, sys::MOQ_ERR_NOMEM),
            (Error::INVAL, sys::MOQ_ERR_INVAL),
            (Error::PROTO, sys::MOQ_ERR_PROTO),
            (Error::CLOSED, sys::MOQ_ERR_CLOSED),
            (Error::WRONG_STATE, sys::MOQ_ERR_WRONG_STATE),
            (Error::STALE_HANDLE, sys::MOQ_ERR_STALE_HANDLE),
            (Error::WRONG_SESSION, sys::MOQ_ERR_WRONG_SESSION),
            (Error::WOULD_BLOCK, sys::MOQ_ERR_WOULD_BLOCK),
            (Error::BUFFER, sys::MOQ_ERR_BUFFER),
            (Error::REQUEST_BLOCKED, sys::MOQ_ERR_REQUEST_BLOCKED),
            (Error::ABI_MISMATCH, sys::MOQ_ERR_ABI_MISMATCH),
            (Error::GOAWAY, sys::MOQ_ERR_GOAWAY),
            (Error::INTERRUPTED, sys::MOQ_ERR_INTERRUPTED),
            (Error::UNSUPPORTED, sys::MOQ_ERR_UNSUPPORTED),
            (Error::INPUT_NOT_CONSUMED, sys::MOQ_ERR_INPUT_NOT_CONSUMED),
            (Error::LIMIT, sys::MOQ_ERR_LIMIT),
            (Error::CAPACITY, sys::MOQ_ERR_CAPACITY),
            (Error::INTERNAL, sys::MOQ_ERR_INTERNAL),
        ];
        assert_eq!(named.len(), 18, "every native code has a constant");
        for (error, code) in named {
            assert_eq!(error.code(), code);
            assert!(error.name().is_some(), "{code} has no name");
            assert!(code < 0, "{code} is not an error code");
        }
    }

    #[test]
    fn codes_round_trip() {
        assert_eq!(Error::from_code(-2), Error::INVAL);
        assert_eq!(Error::from_code(-16), Error::LIMIT);
        assert_eq!(Error::from_code(-17), Error::CAPACITY);
        assert_eq!(
            format!("{:?}", Error::INTERRUPTED),
            "Error(INTERRUPTED, -13)"
        );
        assert_eq!(format!("{:?}", Error::INTERNAL), "Error(INTERNAL, -99)");
        assert_eq!(format!("{:?}", Error::from_code(-1000)), "Error(-1000)");
        assert_eq!(Error::from_code(-1000).code(), -1000);
        assert!(Error::CLOSED.to_string().ends_with("(-4)"));
        assert!(Error::from_code(-1000).to_string().ends_with("(-1000)"));
    }
}
