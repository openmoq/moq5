//! A native buffer reference held by a polled record.

use std::fmt;
use std::marker::PhantomData;
use std::ptr::NonNull;

use crate::{sys, Buffer, Result};

/// A native reference held by a polled action or event: a payload, a chunk
/// or a property block.
///
/// It may share its native count with the session (a zero-copy slice of an
/// input, an object the session still references), and that count is not
/// atomic. So a `Payload` is neither `Send`, `Sync` nor `Clone`, and it
/// cannot outlive the record it came from: its lifetime is the record's
/// exclusive borrow of the session, which ends before any advancing call.
/// Dropping it releases the reference, on the session's thread.
///
/// It leaves the session only as a [`Buffer`] with storage of its own:
/// [`into_buffer`](Payload::into_buffer) always copies.
pub struct Payload<'record> {
    raw: NonNull<sys::moq_rcbuf_t>,
    /// The record's borrow of the session; `*const ()` keeps the type off
    /// other threads.
    _record: PhantomData<(&'record (), *const ())>,
}

impl<'record> Payload<'record> {
    /// Adopt the owned reference a polled record carried, or `None` for a
    /// null field.
    ///
    /// # Safety
    /// `raw`, when non-null, is a live reference the caller owns (taken out of
    /// a polled record, whose field the caller then clears), and the session
    /// it came from stays exclusively borrowed for `'record`.
    pub(super) unsafe fn adopt(raw: *mut sys::moq_rcbuf_t) -> Option<Payload<'record>> {
        NonNull::new(raw).map(|raw| Payload {
            raw,
            _record: PhantomData,
        })
    }

    /// The bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        // SAFETY: the reference this payload owns keeps the bytes alive and
        // unchanged; rcbuf data is immutable once created.
        unsafe {
            let data = sys::moq_rcbuf_data(self.raw.as_ptr());
            let len = sys::moq_rcbuf_len(self.raw.as_ptr());
            if data.is_null() || len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(data, len)
            }
        }
    }

    /// Length in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        // SAFETY: a live reference.
        unsafe { sys::moq_rcbuf_len(self.raw.as_ptr()) }
    }

    /// No bytes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Leave the session as a [`Buffer`] with its own storage: the bytes are
    /// copied, always. The native reference is released either way.
    ///
    /// # Errors
    /// [`Error::NOMEM`](crate::Error::NOMEM) when the copy cannot be
    /// allocated; the reference is still released.
    pub fn into_buffer(self) -> Result<Buffer> {
        Buffer::copy_from(self.as_bytes())
        // `self` drops here, releasing the native reference whether or not
        // the copy succeeded.
    }
}

impl Drop for Payload<'_> {
    fn drop(&mut self) {
        // SAFETY: the one reference this payload owns, released once, on the
        // thread that holds the session's exclusive borrow.
        unsafe { sys::moq_rcbuf_decref(self.raw.as_ptr()) }
    }
}

// The length, not the pointer.
impl fmt::Debug for Payload<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Payload").field("len", &self.len()).finish()
    }
}
