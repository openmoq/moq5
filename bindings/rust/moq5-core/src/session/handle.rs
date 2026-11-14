//! Request handles, each bound to the session that issued it.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Error, Result};

/// The identity of one [`Session`](crate::Session) owner, recorded in every
/// handle it issues. A process-wide counter: two owners never share one.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct OwnerId(u64);

impl OwnerId {
    pub(super) fn next() -> OwnerId {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        // Relaxed: only uniqueness matters, and fetch_add gives that.
        OwnerId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// What every handle type shares: its native value and its issuing owner.
pub(super) trait Handle: Copy {
    fn owner(self) -> OwnerId;
    fn value(self) -> u64;
}

/// The native value of `handle` when `owner` issued it; a handle of another
/// session is refused before it reaches C.
pub(super) fn checked<H: Handle>(handle: H, owner: OwnerId) -> Result<u64> {
    if handle.owner() == owner {
        Ok(handle.value())
    } else {
        Err(Error::WRONG_SESSION)
    }
}

macro_rules! handle {
    ($(#[$doc:meta])* $name:ident, $raw:ident, $trace:ident) => {
        $(#[$doc])*
        ///
        /// A value: copying it copies the identity. It names the request only
        /// on the session that issued it; any other session refuses it with
        /// [`Error::WRONG_SESSION`](crate::Error::WRONG_SESSION) before the
        /// call reaches C. After the request ends, the session answers it
        /// with [`Error::STALE_HANDLE`](crate::Error::STALE_HANDLE).
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name {
            value: u64,
            owner: OwnerId,
        }

        impl $name {
            pub(super) fn new(raw: crate::sys::$raw, owner: OwnerId) -> $name {
                $name { value: raw._opaque, owner }
            }

            pub(super) fn raw(self) -> crate::sys::$raw {
                crate::sys::$raw { _opaque: self.value }
            }

            /// A stable number for logs and traces; it carries no meaning
            /// beyond telling handles of one session apart.
            #[must_use]
            pub fn id_for_trace(self) -> u64 {
                // SAFETY: a pure function of the value.
                unsafe { crate::sys::$trace(self.raw()) }
            }
        }

        impl Handle for $name {
            fn owner(self) -> OwnerId {
                self.owner
            }

            fn value(self) -> u64 {
                self.value
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({:#x})"), self.value)
            }
        }
    };
}

handle!(
    /// A subscription: one this side requested, or one a peer asked of it.
    Subscription,
    moq_subscription_t,
    moq_subscription_id_for_trace
);
handle!(
    /// A publisher-initiated subscription (PUBLISH), on either side.
    Publication,
    moq_publication_t,
    moq_publication_id_for_trace
);
handle!(
    /// A FETCH, on either side.
    Fetch,
    moq_fetch_t,
    moq_fetch_id_for_trace
);
handle!(
    /// An outgoing subgroup: one data stream of a subscription or
    /// publication this side publishes.
    Subgroup,
    moq_subgroup_handle_t,
    moq_subgroup_id_for_trace
);
handle!(
    /// A namespace advertisement (PUBLISH_NAMESPACE), on either side.
    Announcement,
    moq_announcement_t,
    moq_announcement_id_for_trace
);
handle!(
    /// A namespace subscription (SUBSCRIBE_NAMESPACE), on either side.
    NamespaceSubscription,
    moq_ns_sub_handle_t,
    moq_ns_sub_handle_id_for_trace
);
handle!(
    /// A fill child of a subscription, on either side.
    Fill,
    moq_fill_t,
    moq_fill_id_for_trace
);
handle!(
    /// A TRACK_STATUS request, on either side.
    TrackStatusRequest,
    moq_track_status_handle_t,
    moq_track_status_handle_id_for_trace
);
handle!(
    /// A SUBSCRIBE_TRACKS request, on either side.
    TrackSubscription,
    moq_track_sub_handle_t,
    moq_track_sub_handle_id_for_trace
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owners_are_distinct_and_checked() {
        let (a, b) = (OwnerId::next(), OwnerId::next());
        assert_ne!(a, b);
        let raw = crate::sys::moq_fetch_t { _opaque: 0x1234 };
        let fetch = Fetch::new(raw, a);
        assert_eq!(checked(fetch, a), Ok(0x1234));
        assert_eq!(checked(fetch, b), Err(Error::WRONG_SESSION));
        assert_eq!(format!("{fetch:?}"), "Fetch(0x1234)");
        assert_ne!(
            Fetch::new(raw, b),
            fetch,
            "the owner is part of the identity"
        );
    }
}
