//! Value types of the service tier: the outcome of a bounded wait and the
//! delivery a received object came from.

use crate::sys;

/// Outcome of a blocking wait that did not fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitResult {
    /// The awaited level holds, or the endpoint woke (`MOQ_OK`).
    Ready,
    /// The timeout elapsed first (`MOQ_DONE`).
    TimedOut,
}

/// Which delivery an object or record came from (`MOQ_MEDIA_DELIVERY_*`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeliveryOrigin {
    /// The live subscription.
    Live,
    /// A fill, named by its initiating request ID.
    Fill,
    /// An origin this crate does not know.
    Other(u32),
}

impl DeliveryOrigin {
    pub(crate) fn from_raw(code: u32) -> DeliveryOrigin {
        match code {
            sys::MOQ_MEDIA_DELIVERY_LIVE => DeliveryOrigin::Live,
            sys::MOQ_MEDIA_DELIVERY_FILL => DeliveryOrigin::Fill,
            code => DeliveryOrigin::Other(code),
        }
    }

    /// The `MOQ_MEDIA_DELIVERY_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            DeliveryOrigin::Live => sys::MOQ_MEDIA_DELIVERY_LIVE,
            DeliveryOrigin::Fill => sys::MOQ_MEDIA_DELIVERY_FILL,
            DeliveryOrigin::Other(code) => code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_sets_round_trip() {
        assert_eq!(DeliveryOrigin::from_raw(2), DeliveryOrigin::Fill);
        assert_eq!(DeliveryOrigin::from_raw(9).code(), 9);
    }
}
