//! Conversions at the C boundary: byte spans, arrays and result codes.
//! Private to this crate; each crate of the bindings keeps its own copy.
//!
//! Two views of a native (pointer, length) pair exist because two contracts
//! exist. [`bytes`] and [`array`] read storage this crate owns or built
//! (a buffer, a namespace), where a null pointer only ever means empty. The
//! `record_*` forms read what a library record states, where a null pointer
//! with a nonzero length is outside the record's contract and is refused
//! rather than read as empty.

use std::slice;

use crate::error::{Error, Result};
use crate::sys;

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

/// View a libmoq byte span as a slice (empty for a null span, whatever its
/// length).
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

/// View a libmoq (pointer, count) array as a slice (empty for a null
/// pointer, whatever the count).
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

/// View a (pointer, count) array a library record states as a slice. A
/// null pointer with a nonzero count is [`Error::ABI_MISMATCH`].
///
/// # Safety
/// When `ptr` is non-null it must be valid for `len` elements for `'a`.
pub(crate) unsafe fn record_array<'a, T>(ptr: *const T, len: usize) -> Result<&'a [T]> {
    if len == 0 {
        Ok(&[])
    } else if ptr.is_null() {
        Err(Error::ABI_MISMATCH)
    } else {
        // SAFETY: the caller's contract; non-null with this length.
        Ok(unsafe { slice::from_raw_parts(ptr, len) })
    }
}

/// [`record_array`] for a byte range.
///
/// # Safety
/// As for [`record_array`].
pub(crate) unsafe fn record_bytes<'a>(data: *const u8, len: usize) -> Result<&'a [u8]> {
    // SAFETY: the caller's contract.
    unsafe { record_array(data, len) }
}

/// [`record_array`] for a `moq_bytes_t`.
///
/// # Safety
/// As for [`record_array`].
pub(crate) unsafe fn record_span<'a>(span: sys::moq_bytes_t) -> Result<&'a [u8]> {
    // SAFETY: the caller's contract.
    unsafe { record_array(span.data, span.len) }
}

/// Map a libmoq result code: `MOQ_OK` is success, anything else an error.
pub(crate) fn check(code: sys::moq_result_t) -> Result<()> {
    if code == sys::MOQ_OK {
        Ok(())
    } else {
        Err(Error::from_code(code))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes() {
        assert_eq!(check(sys::MOQ_OK), Ok(()));
        assert_eq!(check(sys::MOQ_ERR_CLOSED), Err(Error::CLOSED));
    }

    #[test]
    fn spans() {
        assert!(span(&[]).data.is_null());
        let text = String::from("abc");
        // SAFETY: the span borrows `text`, alive for the call.
        assert_eq!(unsafe { bytes(span(text.as_bytes())) }, b"abc");
        // SAFETY: a null span.
        assert!(unsafe { bytes(sys::moq_bytes_t::default()) }.is_empty());
        // SAFETY: a null array.
        assert!(unsafe { array::<u8>(std::ptr::null(), 4) }.is_empty());
    }

    #[test]
    fn record_views_refuse_a_null_pointer_with_a_length() {
        // SAFETY: null pointers of length zero, a null pointer the checked
        // form refuses before reading, and a static array.
        unsafe {
            assert_eq!(record_bytes(std::ptr::null(), 0), Ok(&[][..]));
            assert_eq!(record_bytes(std::ptr::null(), 3), Err(Error::ABI_MISMATCH));
            assert_eq!(record_bytes(b"xyz".as_ptr(), 2), Ok(&b"xy"[..]));
            assert_eq!(record_span(span(b"ab")), Ok(&b"ab"[..]));
            assert_eq!(record_array::<u32>(std::ptr::null(), 0), Ok(&[][..]));
            assert_eq!(
                record_array::<u32>(std::ptr::null(), 1),
                Err(Error::ABI_MISMATCH)
            );
        }
    }
}
