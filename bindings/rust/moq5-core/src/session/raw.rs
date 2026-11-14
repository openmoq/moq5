//! The coverage of sized records and exact durations at the session's C
//! boundary. Spans, arrays and result codes are the crate's own
//! ([`crate::ffi`]), re-exported here for the session's modules.

use std::time::Duration;

use crate::{Error, Result};

pub(super) use crate::ffi::{check, record_array, record_bytes, record_span, span};

/// Whether a record's stamped size reaches the end of a field.
pub(super) fn covers(stamp: u32, field_end: usize) -> bool {
    stamp as usize >= field_end
}

/// A sized output the library stamped: at least its floor, at most what was
/// passed in.
pub(super) fn stamp(stamp: u32, floor: usize, passed: usize) -> Result<()> {
    if covers(stamp, floor) && stamp as usize <= passed {
        Ok(())
    } else {
        Err(Error::ABI_MISMATCH)
    }
}

/// A duration as the exact milliseconds C takes; a remainder or an
/// overflow is [`Error::INVAL`], never rounded.
pub(super) fn millis(duration: Duration) -> Result<u64> {
    let nanos = duration.as_nanos();
    if nanos % 1_000_000 != 0 {
        return Err(Error::INVAL);
    }
    u64::try_from(nanos / 1_000_000).map_err(|_| Error::INVAL)
}

/// A duration as the exact microseconds C takes; a remainder or an
/// overflow is [`Error::INVAL`], never rounded.
pub(super) fn micros(duration: Duration) -> Result<u64> {
    let nanos = duration.as_nanos();
    if nanos % 1_000 != 0 {
        return Err(Error::INVAL);
    }
    u64::try_from(nanos / 1_000).map_err(|_| Error::INVAL)
}

/// The end of a field in a sized record: what a stamp must reach for the
/// field to be read.
macro_rules! field_end {
    ($record:ty, $field:ident, $field_type:ty) => {
        std::mem::offset_of!($record, $field) + std::mem::size_of::<$field_type>()
    };
}
pub(super) use field_end;

/// Refuse a record whose stamp is below its frozen floor: an older library
/// whose record may have had another shape.
pub(super) fn floor(stamp: u32, floor: usize) -> Result<()> {
    if covers(stamp, floor) {
        Ok(())
    } else {
        Err(Error::ABI_MISMATCH)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_and_durations() {
        assert!(covers(8, 8));
        assert!(!covers(7, 8));
        assert_eq!(floor(7, 8), Err(Error::ABI_MISMATCH));
        assert_eq!(stamp(16, 8, 16), Ok(()));
        assert_eq!(stamp(17, 8, 16), Err(Error::ABI_MISMATCH));
        assert_eq!(
            field_end!(crate::sys::moq_bytes_t, len, usize),
            std::mem::size_of::<crate::sys::moq_bytes_t>()
        );
        assert_eq!(millis(Duration::from_micros(1500)), Err(Error::INVAL));
        assert_eq!(millis(Duration::from_millis(7)), Ok(7));
        assert_eq!(micros(Duration::from_nanos(1)), Err(Error::INVAL));
    }
}
