use std::ffi::c_void;
use std::ops::Range;
use std::ptr::NonNull;
use std::{fmt, mem, ptr};

use crate::error::{Error, Result};
use crate::{ffi, sys};

/// An owned reference to a libmoq refcounted byte buffer (`moq_rcbuf_t`):
/// the payload type [`Sender::write`](crate::Sender::write) takes.
///
/// libmoq's refcounts are not atomic, so this type never shares a buffer:
/// there is no `Clone`, and the reference it owns is the only one. It can
/// move to another thread for that same reason, and a successful write
/// transfers it to the service.
///
/// The bytes come from a copy ([`copy_from`](Buffer::copy_from)) or from a
/// container this crate adopts ([`from_vec`](Buffer::from_vec),
/// [`from_boxed`](Buffer::from_boxed) and their range forms; also
/// `TryFrom`). The release that runs when the last reference goes only
/// deallocates that container. It runs on whichever thread drops the final
/// reference, an application thread or the service's network thread, and is
/// correct on both: it runs no application code, reaches no endpoint, and
/// cannot unwind.
pub struct Buffer(NonNull<sys::moq_rcbuf_t>);

// SAFETY: a Buffer is the only reference to its rcbuf, so moving it moves
// sole ownership; nothing touches the refcount through a shared reference.
unsafe impl Send for Buffer {}

unsafe extern "C" fn release_vec(context: *mut c_void, _data: *const u8, _len: usize) {
    // SAFETY: context is the Box<Vec<u8>> from_vec_range leaked; libmoq
    // calls this exactly once, on the final decref. Freeing a Vec cannot
    // unwind.
    drop(unsafe { Box::from_raw(context as *mut Vec<u8>) });
}

unsafe extern "C" fn release_boxed(context: *mut c_void, _data: *const u8, _len: usize) {
    // SAFETY: as for release_vec, with a Box<Box<[u8]>>.
    drop(unsafe { Box::from_raw(context as *mut Box<[u8]>) });
}

fn checked(range: &Range<usize>, len: usize) -> Result<()> {
    if range.start <= range.end && range.end <= len {
        Ok(())
    } else {
        Err(Error::INVAL)
    }
}

impl Buffer {
    /// A buffer holding a copy of `data`.
    pub fn copy_from(data: &[u8]) -> Result<Buffer> {
        let mut raw = ptr::null_mut();
        // SAFETY: data is valid for the call; the default allocator is
        // static.
        ffi::check(unsafe {
            sys::moq_rcbuf_create(
                sys::moq_alloc_default(),
                data.as_ptr(),
                data.len(),
                &mut raw,
            )
        })?;
        NonNull::new(raw).map(Buffer).ok_or(Error::INTERNAL)
    }

    /// Adopt `data` without copying.
    pub fn from_vec(data: Vec<u8>) -> Result<Buffer> {
        let len = data.len();
        Buffer::from_vec_range(data, 0..len)
    }

    /// Adopt `range` of `data` without copying, for instance to skip a
    /// header the container still holds. A range outside `data` is
    /// [`Error::INVAL`].
    pub fn from_vec_range(data: Vec<u8>, range: Range<usize>) -> Result<Buffer> {
        checked(&range, data.len())?;
        let owner = Box::new(data);
        let bytes = &owner[range];
        let (data_ptr, len) = (bytes.as_ptr(), bytes.len());
        let context = Box::into_raw(owner);
        // SAFETY: the Vec's heap block is never touched again until release
        // frees it, so the pointer and length stay valid for the buffer's
        // life; release_vec reclaims exactly the box leaked here.
        let wrapped =
            unsafe { Buffer::wrap(data_ptr, len, Some(release_vec), context as *mut c_void) };
        if wrapped.is_err() {
            // On failure libmoq does not call release: the box is ours again.
            // SAFETY: the pointer leaked above, not yet reclaimed.
            drop(unsafe { Box::from_raw(context) });
        }
        wrapped
    }

    /// Adopt `data` without copying.
    pub fn from_boxed(data: Box<[u8]>) -> Result<Buffer> {
        let len = data.len();
        Buffer::from_boxed_range(data, 0..len)
    }

    /// Adopt `range` of `data` without copying. A range outside `data` is
    /// [`Error::INVAL`].
    pub fn from_boxed_range(data: Box<[u8]>, range: Range<usize>) -> Result<Buffer> {
        checked(&range, data.len())?;
        let owner = Box::new(data);
        let bytes = &owner[range];
        let (data_ptr, len) = (bytes.as_ptr(), bytes.len());
        let context = Box::into_raw(owner);
        // SAFETY: as for from_vec_range.
        let wrapped =
            unsafe { Buffer::wrap(data_ptr, len, Some(release_boxed), context as *mut c_void) };
        if wrapped.is_err() {
            // SAFETY: as for from_vec_range.
            drop(unsafe { Box::from_raw(context) });
        }
        wrapped
    }

    /// `moq_rcbuf_wrap` with the default allocator.
    ///
    /// # Safety
    /// As for [`wrap_raw`](Buffer::wrap_raw).
    unsafe fn wrap(
        data: *const u8,
        len: usize,
        release: sys::moq_rcbuf_release_fn,
        context: *mut c_void,
    ) -> Result<Buffer> {
        let mut raw = ptr::null_mut();
        // SAFETY: the caller's contract; the default allocator is static.
        let code = unsafe {
            sys::moq_rcbuf_wrap(
                sys::moq_alloc_default(),
                data,
                len,
                release,
                context,
                &mut raw,
            )
        };
        ffi::check(code)?;
        NonNull::new(raw).map(Buffer).ok_or(Error::INTERNAL)
    }

    /// Wrap external bytes without copying (`moq_rcbuf_wrap`). `release`,
    /// when given, runs exactly once with `context` when the last reference
    /// goes; on failure it is not called and `context` is still the
    /// caller's.
    ///
    /// # Safety
    /// `data` must stay valid and unmodified for `len` bytes until `release`
    /// runs (or forever, without one). `release` must meet what the safe
    /// constructors guarantee: it may run on any thread that drops the final
    /// reference, including the service's network thread; it must not
    /// re-enter any object of this crate or any endpoint; it must not
    /// unwind. A buffer adopted this way is outside the crate's
    /// no-application-code-on-native-threads invariant: the caller
    /// re-establishes it.
    pub unsafe fn wrap_raw(
        data: *const u8,
        len: usize,
        release: sys::moq_rcbuf_release_fn,
        context: *mut c_void,
    ) -> Result<Buffer> {
        // SAFETY: the caller's contract.
        unsafe { Buffer::wrap(data, len, release, context) }
    }

    /// The bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: the data lives as long as the rcbuf, which `self` owns.
        unsafe {
            ffi::bytes(sys::moq_bytes_t {
                data: sys::moq_rcbuf_data(self.0.as_ptr()),
                len: sys::moq_rcbuf_len(self.0.as_ptr()),
            })
        }
    }

    /// Length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        // SAFETY: a live rcbuf.
        unsafe { sys::moq_rcbuf_len(self.0.as_ptr()) }
    }

    /// No bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The underlying `moq_rcbuf_t`, still owned by `self`.
    #[must_use]
    pub fn as_ptr(&self) -> *mut sys::moq_rcbuf_t {
        self.0.as_ptr()
    }

    /// Give up the reference without releasing it.
    #[must_use]
    pub fn into_raw(self) -> *mut sys::moq_rcbuf_t {
        let raw = self.0.as_ptr();
        mem::forget(self);
        raw
    }

    /// Take ownership of one reference.
    ///
    /// # Safety
    /// `raw` must be a live rcbuf reference the caller owns exclusively: no
    /// other reference may share it, and its release (if any) must meet the
    /// contract of [`wrap_raw`](Buffer::wrap_raw).
    ///
    /// # Panics
    /// If `raw` is null.
    pub unsafe fn from_raw(raw: *mut sys::moq_rcbuf_t) -> Buffer {
        Buffer(NonNull::new(raw).expect("a null rcbuf cannot be adopted"))
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        // SAFETY: the sole reference; decref releases it.
        unsafe { sys::moq_rcbuf_decref(self.0.as_ptr()) }
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl TryFrom<Vec<u8>> for Buffer {
    type Error = Error;

    fn try_from(data: Vec<u8>) -> Result<Buffer> {
        Buffer::from_vec(data)
    }
}

impl TryFrom<Box<[u8]>> for Buffer {
    type Error = Error;

    fn try_from(data: Box<[u8]>) -> Result<Buffer> {
        Buffer::from_boxed(data)
    }
}

// The length, not the pointer a derive would print.
impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buffer").field("len", &self.len()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy() {
        let buffer = Buffer::copy_from(b"abc").expect("a copy");
        assert_eq!(buffer.as_ref(), b"abc");
        let raw = buffer.into_raw();
        // SAFETY: the reference into_raw gave up.
        let buffer = unsafe { Buffer::from_raw(raw) };
        assert_eq!(buffer.len(), 3);
        assert!(Buffer::copy_from(&[]).expect("an empty copy").is_empty());
    }

    #[test]
    fn adopts_containers() {
        let buffer = Buffer::from_vec(vec![1, 2, 3, 4]).expect("a vec");
        assert_eq!(buffer.as_slice(), [1, 2, 3, 4]);
        let buffer = Buffer::from_vec_range(vec![1, 2, 3, 4], 1..3).expect("a range");
        assert_eq!(buffer.as_slice(), [2, 3]);
        let buffer = Buffer::from_boxed(vec![9u8; 5].into_boxed_slice()).expect("a box");
        assert_eq!(buffer.len(), 5);
        let buffer = Buffer::from_boxed_range(vec![7, 8, 9].into_boxed_slice(), 3..3)
            .expect("an empty range");
        assert!(buffer.is_empty());
        let buffer: Buffer = vec![4, 5].try_into().expect("TryFrom<Vec>");
        assert_eq!(buffer.as_slice(), [4, 5]);
        let buffer: Buffer = vec![6u8]
            .into_boxed_slice()
            .try_into()
            .expect("TryFrom<Box>");
        assert_eq!(buffer.as_slice(), [6]);
        assert_eq!(
            Buffer::from_vec_range(vec![1, 2], 1..3).unwrap_err(),
            Error::INVAL
        );
        let (start, end) = (2, 1);
        assert_eq!(
            Buffer::from_vec_range(vec![1, 2], start..end).unwrap_err(),
            Error::INVAL
        );
        assert_eq!(
            Buffer::from_boxed_range(vec![1u8].into_boxed_slice(), 0..2).unwrap_err(),
            Error::INVAL
        );
        assert_eq!(
            format!("{:?}", Buffer::from_vec(vec![0; 2]).expect("a vec")),
            "Buffer { len: 2 }"
        );
    }

    #[test]
    fn raw_release_runs_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static RELEASES: AtomicUsize = AtomicUsize::new(0);
        unsafe extern "C" fn count(context: *mut c_void, data: *const u8, len: usize) {
            assert_eq!(context as usize, 42);
            assert_eq!(len, 2);
            assert!(!data.is_null());
            RELEASES.fetch_add(1, Ordering::SeqCst);
        }
        static DATA: [u8; 2] = [5, 6];
        // SAFETY: static bytes; the release only counts.
        let buffer = unsafe { Buffer::wrap_raw(DATA.as_ptr(), 2, Some(count), 42 as *mut c_void) }
            .expect("a wrap");
        assert_eq!(buffer.as_slice(), [5, 6]);
        assert_eq!(RELEASES.load(Ordering::SeqCst), 0);
        drop(buffer);
        assert_eq!(RELEASES.load(Ordering::SeqCst), 1, "released exactly once");
    }
}
