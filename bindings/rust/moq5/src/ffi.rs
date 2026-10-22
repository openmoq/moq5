//! Conversions at the C boundary: byte spans, arrays, durations, result
//! codes and the `struct_size` stamps of sized records.

use std::slice;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::sys;
use crate::types::WaitResult;

/// Borrow `bytes` as a libmoq byte span. An empty slice becomes the null
/// span, which libmoq reads as "absent".
pub(crate) fn span(bytes: &[u8]) -> sys::moq_bytes_t {
    if bytes.is_empty() {
        sys::moq_bytes_t::default()
    } else {
        sys::moq_bytes_t {
            data: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
}

/// Borrow an optional string as a span; `None` is the null span.
pub(crate) fn optional_span(text: Option<&str>) -> sys::moq_bytes_t {
    text.map(|text| span(text.as_bytes())).unwrap_or_default()
}

/// View a libmoq byte span as a slice (empty for a null span).
///
/// # Safety
/// `bytes` must describe memory valid for `'a`.
pub(crate) unsafe fn bytes<'a>(bytes: sys::moq_bytes_t) -> &'a [u8] {
    if bytes.data.is_null() || bytes.len == 0 {
        &[]
    } else {
        // SAFETY: the caller's contract.
        unsafe { slice::from_raw_parts(bytes.data, bytes.len) }
    }
}

/// View a libmoq (pointer, count) array as a slice.
///
/// # Safety
/// `ptr..ptr+len` must be valid for `'a`.
pub(crate) unsafe fn array<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        // SAFETY: the caller's contract.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

/// A duration as the exact microseconds libmoq takes. A sub-microsecond
/// remainder or a value above `u64::MAX` microseconds is [`Error::INVAL`]:
/// nothing is rounded or saturated, so a positive bound never becomes the
/// zero that selects a library default.
pub(crate) fn micros(duration: Duration) -> Result<u64> {
    let nanos = duration.as_nanos();
    if nanos % 1_000 != 0 {
        return Err(Error::INVAL);
    }
    u64::try_from(nanos / 1_000).map_err(|_| Error::INVAL)
}

/// The stamped prefix of a sized output reaches the end of a field.
pub(crate) fn covers(stamp: u32, field_end: usize) -> bool {
    stamp as usize >= field_end
}

/// Validate the `struct_size` the library stamped on a sized output: at
/// least the frozen floor, at most what this crate passed in.
pub(crate) fn check_stamp(stamp: u32, floor: usize, passed: usize) -> Result<()> {
    if covers(stamp, floor) && stamp as usize <= passed {
        Ok(())
    } else {
        Err(Error::ABI_MISMATCH)
    }
}

/// Map a libmoq result code: `MOQ_OK` is success, anything else an error.
pub(crate) fn check(code: sys::moq_result_t) -> Result<()> {
    if code == sys::MOQ_OK {
        Ok(())
    } else {
        Err(Error::from_code(code))
    }
}

/// Map the result of a bounded wait.
pub(crate) fn wait_result(code: sys::moq_result_t) -> Result<WaitResult> {
    match code {
        sys::MOQ_OK => Ok(WaitResult::Ready),
        sys::MOQ_DONE => Ok(WaitResult::TimedOut),
        code => Err(Error::from_code(code)),
    }
}

/// The end of a field in a sized record: what a stamp must reach to make
/// the field readable.
macro_rules! field_end {
    ($record:ty, $field:ident, $field_type:ty) => {
        std::mem::offset_of!($record, $field) + std::mem::size_of::<$field_type>()
    };
}
pub(crate) use field_end;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn micros_is_exact() {
        assert_eq!(micros(Duration::ZERO), Ok(0));
        assert_eq!(micros(Duration::from_micros(1)), Ok(1));
        assert_eq!(micros(Duration::from_millis(1500)), Ok(1_500_000));
        assert_eq!(micros(Duration::from_micros(u64::MAX)), Ok(u64::MAX));
        assert_eq!(micros(Duration::from_nanos(999)), Err(Error::INVAL));
        assert_eq!(micros(Duration::from_nanos(1_000_001)), Err(Error::INVAL));
        assert_eq!(
            micros(Duration::from_micros(u64::MAX) + Duration::from_micros(1)),
            Err(Error::INVAL)
        );
        assert_eq!(micros(Duration::MAX), Err(Error::INVAL));
    }

    #[test]
    fn stamps() {
        assert_eq!(check_stamp(16, 16, 24), Ok(()));
        assert_eq!(check_stamp(24, 16, 24), Ok(()));
        assert_eq!(check_stamp(15, 16, 24), Err(Error::ABI_MISMATCH));
        assert_eq!(check_stamp(25, 16, 24), Err(Error::ABI_MISMATCH));
        assert!(covers(24, 24));
        assert!(!covers(23, 24));
        assert_eq!(
            field_end!(sys::moq_bytes_t, len, usize),
            std::mem::size_of::<sys::moq_bytes_t>()
        );
    }

    #[test]
    fn codes() {
        assert_eq!(check(sys::MOQ_OK), Ok(()));
        assert_eq!(check(sys::MOQ_ERR_CLOSED), Err(Error::CLOSED));
        assert_eq!(wait_result(sys::MOQ_OK), Ok(WaitResult::Ready));
        assert_eq!(wait_result(sys::MOQ_DONE), Ok(WaitResult::TimedOut));
        assert_eq!(wait_result(sys::MOQ_ERR_CLOSED), Err(Error::CLOSED));
    }

    #[test]
    fn spans() {
        assert!(span(&[]).data.is_null());
        let text = String::from("abc");
        assert_eq!(optional_span(Some(&text)).len, 3);
        assert!(optional_span(None).data.is_null());
        // SAFETY: the span borrows `text`, alive for the call.
        assert_eq!(unsafe { bytes(span(text.as_bytes())) }, b"abc");
        // SAFETY: a null span.
        assert!(unsafe { bytes(sys::moq_bytes_t::default()) }.is_empty());
        // SAFETY: a null array.
        assert!(unsafe { array::<u8>(std::ptr::null(), 4) }.is_empty());
    }
}
