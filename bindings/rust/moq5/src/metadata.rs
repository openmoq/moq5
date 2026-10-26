//! LOC-04 object metadata (draft 22): what a sender writes with an object,
//! and the effective metadata a receiver reports for one.

use std::fmt;

use crate::error::{Error, Result};
use crate::types::TimeDomain;
use crate::{ffi, sys};

/// A LOC-04 frame marking: the one to four bytes of its payload, carried
/// verbatim (no normalization).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Marking {
    bytes: [u8; 4],
    len: u8,
}

impl Marking {
    /// A marking of `bytes`; anything but one to four bytes is
    /// [`Error::INVAL`], since the record cannot carry it.
    pub fn new(bytes: &[u8]) -> Result<Marking> {
        if bytes.is_empty() || bytes.len() > 4 {
            return Err(Error::INVAL);
        }
        let mut marking = Marking {
            bytes: [0; 4],
            len: bytes.len() as u8,
        };
        marking.bytes[..bytes.len()].copy_from_slice(bytes);
        Ok(marking)
    }

    /// The marking's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

// The bytes, not the padded array a derive would print.
impl fmt::Debug for Marking {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Marking").field(&self.as_bytes()).finish()
    }
}

/// The LOC-04 metadata written with one draft-22 RAW object
/// ([`SendObject::metadata`]): the wire input itself, every field as raw
/// LOC values. The timestamp is required. With a timescale it is
/// application media time in those units per second; without one it is
/// Unix-epoch microseconds. Configuration bytes present here, even empty
/// ones, override the track's.
///
/// It is the application's record, so it is not `#[non_exhaustive]`; the
/// service validates it at the write (an unusable time or invalid LOC input
/// is [`Error::INVAL`]) and copies what it keeps.
///
/// [`SendObject::metadata`]: crate::SendObject::metadata
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LocMetadata {
    /// Timestamp, raw ticks.
    pub timestamp: Option<u64>,
    /// Ticks per second; its presence makes the timestamp media time.
    pub timescale: Option<u64>,
    /// Frame marking.
    pub marking: Option<Marking>,
    /// Audio level byte: the V bit and the seven-bit level, raw.
    pub audio_level: Option<u8>,
    /// Video decoder configuration.
    pub video_config: Option<Vec<u8>>,
    /// Audio decoder configuration.
    pub audio_config: Option<Vec<u8>>,
}

impl LocMetadata {
    /// The full-sized C record, borrowing the configuration bytes from
    /// `self` (valid while `self` is).
    pub(crate) fn raw(&self) -> sys::moq_media_metadata_t {
        let mut loc = sys::moq_loc_metadata_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe { sys::moq_loc_metadata_init_sized(&mut loc, std::mem::size_of_val(&loc)) };
        if let Some(timestamp) = self.timestamp {
            loc.present |= sys::MOQ_LOC_METADATA_HAS_TIMESTAMP;
            loc.timestamp = timestamp;
        }
        if let Some(timescale) = self.timescale {
            loc.present |= sys::MOQ_LOC_METADATA_HAS_TIMESCALE;
            loc.timescale = timescale;
        }
        if let Some(marking) = self.marking {
            loc.present |= sys::MOQ_LOC_METADATA_HAS_MARKING;
            loc.marking_len = marking.len;
            loc.marking = marking.bytes;
        }
        if let Some(level) = self.audio_level {
            loc.present |= sys::MOQ_LOC_METADATA_HAS_AUDIO_LEVEL;
            loc.audio_level = level;
        }
        if let Some(config) = &self.video_config {
            loc.present |= sys::MOQ_LOC_METADATA_HAS_VIDEO_CONFIG;
            loc.video_config = ffi::span(config);
        }
        if let Some(config) = &self.audio_config {
            loc.present |= sys::MOQ_LOC_METADATA_HAS_AUDIO_CONFIG;
            loc.audio_config = ffi::span(config);
        }
        sys::moq_media_metadata_t {
            struct_size: std::mem::size_of::<sys::moq_media_metadata_t>() as u32,
            loc,
            ..Default::default()
        }
    }
}

/// The effective LOC-04 metadata of a received draft-22 object, borrowed
/// from it: the object's own fields, and for the timescale and decoder
/// configurations what the track declared when the object has none (an
/// empty configuration present on the object still overrides the track's).
/// No clock is inferred: [`time_us`](ObjectMetadata::time_us) is reported
/// only when the timestamp converts.
#[derive(Clone, Copy)]
pub struct ObjectMetadata<'object> {
    raw: &'object sys::moq_media_metadata_t,
}

impl<'object> ObjectMetadata<'object> {
    /// A view of `raw`, when its stamp covers the frozen record (zero is a
    /// legacy object's: no metadata).
    pub(crate) fn from_raw(raw: &'object sys::moq_media_metadata_t) -> Option<Self> {
        let reported = ffi::covers(raw.struct_size, sys::MOQ_MEDIA_METADATA_V0_SIZE)
            && ffi::covers(raw.loc.struct_size, sys::MOQ_LOC_METADATA_V0_SIZE);
        reported.then_some(ObjectMetadata { raw })
    }

    fn has(&self, bit: u32) -> bool {
        self.raw.loc.present & bit != 0
    }

    /// The clock [`time_us`](ObjectMetadata::time_us) is in.
    #[must_use]
    pub fn time_domain(&self) -> TimeDomain {
        TimeDomain::from_raw(self.raw.time_domain)
    }
    /// The timestamp in microseconds of its domain, when it converts; a raw
    /// zero or an overflowing value stays only in
    /// [`timestamp`](ObjectMetadata::timestamp).
    #[must_use]
    pub fn time_us(&self) -> Option<u64> {
        (self.raw.has_time_us != 0).then_some(self.raw.time_us)
    }
    /// Whether the object is independently decodable, as its marking says;
    /// `None` when no marking says.
    #[must_use]
    pub fn independent(&self) -> Option<bool> {
        (self.raw.independence_known != 0).then_some(self.raw.independent != 0)
    }
    /// Timestamp, raw ticks.
    #[must_use]
    pub fn timestamp(&self) -> Option<u64> {
        self.has(sys::MOQ_LOC_METADATA_HAS_TIMESTAMP)
            .then_some(self.raw.loc.timestamp)
    }
    /// Ticks per second.
    #[must_use]
    pub fn timescale(&self) -> Option<u64> {
        self.has(sys::MOQ_LOC_METADATA_HAS_TIMESCALE)
            .then_some(self.raw.loc.timescale)
    }
    /// Frame marking bytes.
    #[must_use]
    pub fn marking(&self) -> Option<&'object [u8]> {
        if !self.has(sys::MOQ_LOC_METADATA_HAS_MARKING) {
            return None;
        }
        let len = usize::from(self.raw.loc.marking_len).min(self.raw.loc.marking.len());
        Some(&self.raw.loc.marking[..len])
    }
    /// Audio level byte, raw.
    #[must_use]
    pub fn audio_level(&self) -> Option<u8> {
        self.has(sys::MOQ_LOC_METADATA_HAS_AUDIO_LEVEL)
            .then_some(self.raw.loc.audio_level)
    }
    /// Video decoder configuration (present may be empty).
    #[must_use]
    pub fn video_config(&self) -> Option<&'object [u8]> {
        if !self.has(sys::MOQ_LOC_METADATA_HAS_VIDEO_CONFIG) {
            return None;
        }
        // SAFETY: the span borrows the object's properties, owned by the
        // object for 'object.
        Some(unsafe { ffi::bytes(self.raw.loc.video_config) })
    }
    /// Audio decoder configuration (present may be empty).
    #[must_use]
    pub fn audio_config(&self) -> Option<&'object [u8]> {
        if !self.has(sys::MOQ_LOC_METADATA_HAS_AUDIO_CONFIG) {
            return None;
        }
        // SAFETY: as for video_config.
        Some(unsafe { ffi::bytes(self.raw.loc.audio_config) })
    }
    /// The raw record; its spans borrow the object.
    #[must_use]
    pub fn raw(&self) -> &'object sys::moq_media_metadata_t {
        self.raw
    }
}

// The values, not the raw spans a derive would print.
impl fmt::Debug for ObjectMetadata<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectMetadata")
            .field("time_domain", &self.time_domain())
            .field("time_us", &self.time_us())
            .field("timestamp", &self.timestamp())
            .field("timescale", &self.timescale())
            .field("marking", &self.marking())
            .field("independent", &self.independent())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markings_carry_one_to_four_bytes() {
        assert_eq!(Marking::new(&[]), Err(Error::INVAL));
        assert_eq!(Marking::new(&[1, 2, 3, 4, 5]), Err(Error::INVAL));
        let marking = Marking::new(&[0x20, 7]).expect("two bytes");
        assert_eq!(marking.as_bytes(), [0x20, 7]);
        assert_eq!(format!("{marking:?}"), "Marking([32, 7])");
    }

    #[test]
    fn the_record_states_what_is_present() {
        let metadata = LocMetadata {
            timestamp: Some(90_000),
            timescale: Some(90_000),
            marking: Some(Marking::new(&[0x20]).expect("one byte")),
            audio_level: None,
            video_config: Some(vec![]),
            audio_config: Some(vec![1, 2]),
        };
        let raw = metadata.raw();
        assert_eq!(
            raw.struct_size as usize,
            std::mem::size_of::<sys::moq_media_metadata_t>()
        );
        assert!(raw.loc.struct_size as usize >= sys::MOQ_LOC_METADATA_V0_SIZE);
        assert_eq!(
            raw.loc.present,
            sys::MOQ_LOC_METADATA_HAS_TIMESTAMP
                | sys::MOQ_LOC_METADATA_HAS_TIMESCALE
                | sys::MOQ_LOC_METADATA_HAS_MARKING
                | sys::MOQ_LOC_METADATA_HAS_VIDEO_CONFIG
                | sys::MOQ_LOC_METADATA_HAS_AUDIO_CONFIG
        );
        assert_eq!((raw.loc.timestamp, raw.loc.timescale), (90_000, 90_000));
        assert_eq!((raw.loc.marking_len, raw.loc.marking[0]), (1, 0x20));
        assert_eq!(raw.loc.video_config.len, 0, "present and empty");
        assert_eq!(raw.loc.audio_config.len, 2);
        let empty = LocMetadata::default().raw();
        assert_eq!(empty.loc.present, 0);
    }

    #[test]
    fn a_view_reads_what_the_stamps_cover() {
        let mut raw = LocMetadata {
            timestamp: Some(5),
            audio_level: Some(0x85),
            ..LocMetadata::default()
        }
        .raw();
        raw.time_domain = sys::MOQ_MEDIA_TIME_DOMAIN_UNIX_US;
        raw.has_time_us = 1;
        raw.time_us = 5;
        let view = ObjectMetadata::from_raw(&raw).expect("a full record");
        assert_eq!(view.time_domain(), TimeDomain::UnixMicros);
        assert_eq!(view.time_us(), Some(5));
        assert_eq!(view.timestamp(), Some(5));
        assert_eq!(view.timescale(), None);
        assert_eq!(view.audio_level(), Some(0x85));
        assert_eq!(view.independent(), None);
        assert_eq!(view.video_config(), None);
        let legacy = sys::moq_media_metadata_t::default();
        assert!(
            ObjectMetadata::from_raw(&legacy).is_none(),
            "zero is legacy"
        );
        // A stamp short of either frozen record is not read.
        let mut short = raw;
        short.struct_size = (sys::MOQ_MEDIA_METADATA_V0_SIZE - 1) as u32;
        assert!(ObjectMetadata::from_raw(&short).is_none());
        let mut short_loc = raw;
        short_loc.loc.struct_size = (sys::MOQ_LOC_METADATA_V0_SIZE - 1) as u32;
        assert!(ObjectMetadata::from_raw(&short_loc).is_none());
    }
}
