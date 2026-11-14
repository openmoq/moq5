//! Transport stream identities.

use std::fmt;

use crate::sys;

/// The first ref the host may give a stream its peer opened.
const PEER_BASE: u64 = sys::MOQ_STREAM_REF_LOCAL_END;

/// A transport stream as the session names it (`moq_stream_ref_t`).
///
/// Refs come from two ranges. The session names the streams it opens with
/// refs below `2^63`, never reusing one; they arrive in its actions. The host
/// names the streams its peer opens with refs from `2^63` up
/// ([`StreamRef::peer`]), each used once for the session's life. Zero is
/// never a ref. Two refs of one session are the same stream exactly when
/// their values are equal. A ref is a host token: it does not carry its
/// session, and another session may use the same value for another stream;
/// the host keeps each ref with its session.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamRef(u64);

impl StreamRef {
    /// The ref for the `index`th stream the peer opened, counted from zero:
    /// `2^63 + index`. `None` when that is past `u64::MAX`. The host gives
    /// each peer stream its own index and never reuses one with the same
    /// session.
    #[must_use]
    pub const fn peer(index: u64) -> Option<StreamRef> {
        match PEER_BASE.checked_add(index) {
            Some(value) => Some(StreamRef(value)),
            None => None,
        }
    }

    /// A ref from its native value; `None` for zero.
    #[must_use]
    pub const fn from_raw(value: u64) -> Option<StreamRef> {
        if value == 0 {
            None
        } else {
            Some(StreamRef(value))
        }
    }

    /// The native value.
    #[must_use]
    pub const fn as_raw(self) -> u64 {
        self.0
    }

    /// A stream the session opened (from its own range).
    #[must_use]
    pub const fn is_local(self) -> bool {
        self.0 < PEER_BASE
    }

    pub(super) fn raw(self) -> sys::moq_stream_ref_t {
        sys::moq_stream_ref_t { _v: self.0 }
    }

    /// The ref a polled record carries, refusing zero.
    pub(super) fn from_record(raw: sys::moq_stream_ref_t) -> crate::Result<StreamRef> {
        StreamRef::from_raw(raw._v).ok_or(crate::Error::ABI_MISMATCH)
    }
}

impl fmt::Debug for StreamRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_local() {
            write!(f, "StreamRef(local {})", self.0)
        } else {
            write!(f, "StreamRef(peer {})", self.0 - PEER_BASE)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges() {
        let first = StreamRef::peer(0).expect("the first peer ref");
        assert_eq!(first.as_raw(), 1u64 << 63);
        assert!(!first.is_local());
        assert_eq!(
            StreamRef::peer(u64::MAX >> 1),
            StreamRef::from_raw(u64::MAX)
        );
        assert_eq!(StreamRef::peer(1u64 << 63), None);
        assert_eq!(StreamRef::from_raw(0), None);
        let local = StreamRef::from_raw(7).expect("a local ref");
        assert!(local.is_local());
        assert_eq!(format!("{local:?}"), "StreamRef(local 7)");
        assert_eq!(format!("{first:?}"), "StreamRef(peer 0)");
    }
}
