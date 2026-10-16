use std::ffi::c_void;
use std::ops::Range;
use std::ptr::NonNull;

use crate::error::{check, Error, Result};
use crate::sys;
use crate::types::bytes;

/// An owned reference to a libmoq refcounted byte buffer (`moq_rcbuf_t`):
/// the payload type [`MediaSender::write`](crate::MediaSender::write) takes.
///
/// libmoq's refcounts are not atomic, so this type never shares a buffer:
/// there is no `Clone`. It can move to another thread because the reference
/// it owns is the only one.
pub struct RcBuf(NonNull<sys::moq_rcbuf_t>);

// SAFETY: an RcBuf is the only reference to its buffer (see above), so moving
// it moves sole ownership; it has no &self methods that touch the refcount.
unsafe impl Send for RcBuf {}

struct Owner<T>(T);

unsafe extern "C" fn release_owner<T>(ctx: *mut c_void, _data: *const u8, _len: usize) {
    drop(Box::from_raw(ctx as *mut Owner<T>));
}

impl RcBuf {
    /// A buffer holding a copy of `data`.
    pub fn copy_from_slice(data: &[u8]) -> Result<RcBuf> {
        let mut out = std::ptr::null_mut();
        check(unsafe {
            sys::moq_rcbuf_create(
                sys::moq_alloc_default(),
                data.as_ptr(),
                data.len(),
                &mut out,
            )
        })?;
        NonNull::new(out).map(RcBuf).ok_or(Error::INTERNAL)
    }

    /// Wrap `owner`'s bytes without copying. `owner` is dropped when libmoq
    /// releases the buffer -- for a written payload, on the endpoint's
    /// network thread.
    pub fn from_owner<T>(owner: T) -> Result<RcBuf>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        let len = owner.as_ref().len();
        RcBuf::from_owner_range(owner, 0..len)
    }

    /// Wrap `range` of `owner`'s bytes without copying, e.g. to skip a
    /// header. Panics if `range` is out of bounds.
    pub fn from_owner_range<T>(owner: T, range: Range<usize>) -> Result<RcBuf>
    where
        T: AsRef<[u8]> + Send + 'static,
    {
        let owner = Box::new(Owner(owner));
        // The box is never touched again until release, so the slice it
        // yields here stays valid for the buffer's life.
        let data = &owner.0.as_ref()[range];
        let (ptr, len) = (data.as_ptr(), data.len());
        let ctx = Box::into_raw(owner);
        let mut out = std::ptr::null_mut();
        let rc = unsafe {
            sys::moq_rcbuf_wrap(
                sys::moq_alloc_default(),
                ptr,
                len,
                Some(release_owner::<T>),
                ctx as *mut c_void,
                &mut out,
            )
        };
        match NonNull::new(out) {
            Some(buf) if rc == sys::MOQ_OK => Ok(RcBuf(buf)),
            _ => {
                // On failure libmoq does not call release.
                drop(unsafe { Box::from_raw(ctx) });
                Err(if rc == sys::MOQ_OK {
                    Error::INTERNAL
                } else {
                    Error::from_code(rc)
                })
            }
        }
    }

    /// The bytes.
    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            bytes(sys::moq_bytes_t {
                data: sys::moq_rcbuf_data(self.0.as_ptr()),
                len: sys::moq_rcbuf_len(self.0.as_ptr()),
            })
        }
    }

    /// Length in bytes.
    pub fn len(&self) -> usize {
        unsafe { sys::moq_rcbuf_len(self.0.as_ptr()) }
    }

    /// No bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The underlying `moq_rcbuf_t`, still owned by `self`.
    pub fn as_ptr(&self) -> *mut sys::moq_rcbuf_t {
        self.0.as_ptr()
    }

    /// Give up the reference without releasing it.
    pub fn into_raw(self) -> *mut sys::moq_rcbuf_t {
        let raw = self.0.as_ptr();
        std::mem::forget(self);
        raw
    }

    /// Take ownership of one reference.
    ///
    /// # Safety
    /// `raw` must be a live rcbuf reference that the caller owns and that no
    /// other reference shares.
    pub unsafe fn from_raw(raw: *mut sys::moq_rcbuf_t) -> RcBuf {
        RcBuf(NonNull::new(raw).expect("null rcbuf"))
    }
}

impl Drop for RcBuf {
    fn drop(&mut self) {
        unsafe { sys::moq_rcbuf_decref(self.0.as_ptr()) }
    }
}

impl AsRef<[u8]> for RcBuf {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl std::fmt::Debug for RcBuf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RcBuf").field("len", &self.len()).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    struct Tracked(Vec<u8>, Arc<AtomicUsize>);
    impl AsRef<[u8]> for Tracked {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl Drop for Tracked {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn owner_released_once_on_drop() {
        let drops = Arc::new(AtomicUsize::new(0));
        let buf = RcBuf::from_owner_range(Tracked(vec![1, 2, 3, 4], drops.clone()), 1..3).unwrap();
        assert_eq!(buf.as_slice(), [2, 3]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(buf);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn copy() {
        let buf = RcBuf::copy_from_slice(b"abc").unwrap();
        assert_eq!(buf.as_ref(), b"abc");
        let raw = buf.into_raw();
        let buf = unsafe { RcBuf::from_raw(raw) };
        assert_eq!(buf.len(), 3);
        assert!(RcBuf::copy_from_slice(&[]).unwrap().is_empty());
    }
}
