//! Value types shared by every tier: protocol versions, locations, object
//! statuses, group orders and location filters.

use std::ffi::CStr;

use crate::sys;

/// The libmoq version string, e.g. `0.1.0`.
#[must_use]
pub fn libmoq_version() -> &'static str {
    // SAFETY: a static NUL-terminated string.
    unsafe { CStr::from_ptr(sys::moq_version_string()) }
        .to_str()
        .unwrap_or("unknown")
}

/// A MoQ Transport draft version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Version(u32);

impl Version {
    /// draft-ietf-moq-transport-16.
    pub const DRAFT_16: Version = Version(sys::MOQ_VERSION_DRAFT_16);
    /// draft-ietf-moq-transport-18.
    pub const DRAFT_18: Version = Version(sys::MOQ_VERSION_DRAFT_18);
    /// draft-ietf-moq-transport-22.
    pub const DRAFT_22: Version = Version(sys::MOQ_VERSION_DRAFT_22);

    /// A version by draft number. libmoq refuses one it does not support.
    #[must_use]
    pub const fn from_draft(draft: u32) -> Version {
        Version(draft)
    }

    /// The draft number.
    #[must_use]
    pub const fn draft(self) -> u32 {
        self.0
    }

    /// The `moq_version_t` value.
    #[must_use]
    pub const fn as_raw(self) -> sys::moq_version_t {
        self.0
    }

    /// A version from its `moq_version_t` value, known or not.
    #[must_use]
    pub const fn from_raw(raw: sys::moq_version_t) -> Version {
        Version(raw)
    }
}

/// A MoQ Location: a group and an object within it.
///
/// Closed: a Location is a group and an object within it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Location {
    /// The group ID (zero is valid).
    pub group_id: u64,
    /// The object ID within the group (zero is valid).
    pub object_id: u64,
}

/// The order in which a delivery serves groups (`MOQ_GROUP_ORDER_*`). A
/// request that leaves it out inherits it; there is no explicit default.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupOrder {
    /// Lowest group first.
    Ascending,
    /// Highest group first.
    Descending,
}

impl GroupOrder {
    /// The `MOQ_GROUP_ORDER_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            GroupOrder::Ascending => sys::MOQ_GROUP_ORDER_ASCENDING,
            GroupOrder::Descending => sys::MOQ_GROUP_ORDER_DESCENDING,
        }
    }
}

/// Which objects of a track a request covers (`moq_location_filter_cfg_t`),
/// independent of any draft's wire codes. Groups and objects are absolute
/// Locations; an end is inclusive. Which forms a request may carry depends
/// on the negotiated draft (see where each is accepted).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LocationFilter {
    /// No location constraint, stated explicitly: not the same as leaving
    /// the filter out, which inherits.
    Unconstrained,
    /// From the next object published.
    NextObject,
    /// From a group relative to the current one: 0 is the next group, 1
    /// the current group, 2 the one before. The group is clamped to the
    /// valid range.
    RelativeGroup {
        /// The offset back from the next group.
        offset: u64,
    },
    /// From a Location, open-ended.
    AbsoluteStart(Location),
    /// From a Location through every object of `end_group`. A request
    /// with `end_group` below the start group is refused.
    GroupEnd {
        /// The first object covered.
        start: Location,
        /// The last group covered, absolute.
        end_group: u64,
    },
    /// From a Location through `end`, inclusive. An `end` before `start`
    /// within one group is an empty filter, not an invalid one.
    ObjectEnd {
        /// The first object covered.
        start: Location,
        /// The last object covered.
        end: Location,
    },
}

impl LocationFilter {
    /// The C configuration for this filter, sized and stamped by the
    /// library's initializer. It holds no pointer.
    #[must_use]
    pub fn as_raw(self) -> sys::moq_location_filter_cfg_t {
        let mut raw = sys::moq_location_filter_cfg_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe { sys::moq_location_filter_cfg_init_sized(&mut raw, std::mem::size_of_val(&raw)) };
        raw.kind = self.code();
        match self {
            LocationFilter::Unconstrained | LocationFilter::NextObject => {}
            LocationFilter::RelativeGroup { offset } => raw.relative_group_offset = offset,
            LocationFilter::AbsoluteStart(start) => {
                raw.start_group = start.group_id;
                raw.start_object = start.object_id;
            }
            LocationFilter::GroupEnd { start, end_group } => {
                raw.start_group = start.group_id;
                raw.start_object = start.object_id;
                raw.end_group = end_group;
            }
            LocationFilter::ObjectEnd { start, end } => {
                raw.start_group = start.group_id;
                raw.start_object = start.object_id;
                raw.end_group = end.group_id;
                raw.end_object = end.object_id;
            }
        }
        raw
    }

    /// The `MOQ_LOCATION_FILTER_*` kind.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            LocationFilter::Unconstrained => sys::MOQ_LOCATION_FILTER_NONE,
            LocationFilter::NextObject => sys::MOQ_LOCATION_FILTER_NEXT_OBJECT,
            LocationFilter::RelativeGroup { .. } => sys::MOQ_LOCATION_FILTER_RELATIVE_GROUP,
            LocationFilter::AbsoluteStart(_) => sys::MOQ_LOCATION_FILTER_ABSOLUTE_START,
            LocationFilter::GroupEnd { .. } => sys::MOQ_LOCATION_FILTER_GROUP_END,
            LocationFilter::ObjectEnd { .. } => sys::MOQ_LOCATION_FILTER_OBJECT_END,
        }
    }
}

/// A MoQ object status (`moq_object_status_t`, an open set): the semantic
/// status libmoq reports (`MOQ_OBJECT_*`), not a draft's wire code.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ObjectStatus {
    /// An ordinary object; a zero-length one is still media.
    Normal,
    /// End of group: carries no media.
    EndOfGroup,
    /// End of track: carries no media.
    EndOfTrack,
    /// A status this crate does not know.
    Other(u8),
}

impl ObjectStatus {
    /// A status from its `moq_object_status_t` value; an unknown one is
    /// kept as [`ObjectStatus::Other`].
    #[must_use]
    pub fn from_code(code: sys::moq_object_status_t) -> ObjectStatus {
        match code {
            sys::MOQ_OBJECT_NORMAL => ObjectStatus::Normal,
            sys::MOQ_OBJECT_END_OF_GROUP => ObjectStatus::EndOfGroup,
            sys::MOQ_OBJECT_END_OF_TRACK => ObjectStatus::EndOfTrack,
            code => ObjectStatus::Other(code),
        }
    }

    /// The `MOQ_OBJECT_*` value.
    #[must_use]
    pub fn code(self) -> sys::moq_object_status_t {
        match self {
            ObjectStatus::Normal => sys::MOQ_OBJECT_NORMAL,
            ObjectStatus::EndOfGroup => sys::MOQ_OBJECT_END_OF_GROUP,
            ObjectStatus::EndOfTrack => sys::MOQ_OBJECT_END_OF_TRACK,
            ObjectStatus::Other(code) => code,
        }
    }

    /// The object carries no media bytes, by status alone.
    #[must_use]
    pub fn is_status_only(self) -> bool {
        !matches!(self, ObjectStatus::Normal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_string() {
        let version = libmoq_version();
        assert!(!version.is_empty());
        assert!(
            version.split('.').all(|part| part.parse::<u32>().is_ok()),
            "{version}"
        );
    }

    #[test]
    fn versions_convert() {
        assert_eq!(Version::DRAFT_22.as_raw(), sys::MOQ_VERSION_DRAFT_22);
        assert_eq!(
            Version::from_raw(sys::MOQ_VERSION_DRAFT_16),
            Version::DRAFT_16
        );
        assert_eq!(Version::from_raw(99).draft(), 99);
        assert_eq!(Version::from_draft(18), Version::DRAFT_18);
    }

    #[test]
    fn statuses_round_trip() {
        assert_eq!(ObjectStatus::from_code(0), ObjectStatus::Normal);
        assert_eq!(ObjectStatus::from_code(1), ObjectStatus::EndOfGroup);
        assert_eq!(ObjectStatus::from_code(2), ObjectStatus::EndOfTrack);
        // A draft's wire code is not a semantic status.
        assert_eq!(ObjectStatus::from_code(3), ObjectStatus::Other(3));
        assert_eq!(ObjectStatus::from_code(9), ObjectStatus::Other(9));
        for status in [
            ObjectStatus::Normal,
            ObjectStatus::EndOfGroup,
            ObjectStatus::EndOfTrack,
            ObjectStatus::Other(200),
        ] {
            assert_eq!(ObjectStatus::from_code(status.code()), status);
        }
        assert!(ObjectStatus::EndOfTrack.is_status_only());
        assert!(!ObjectStatus::Normal.is_status_only());
        assert!(
            Location::default()
                < Location {
                    group_id: 0,
                    object_id: 1
                }
        );
    }

    #[test]
    fn filters_build_their_records() {
        let raw = LocationFilter::GroupEnd {
            start: Location {
                group_id: 4,
                object_id: 2,
            },
            end_group: 9,
        }
        .as_raw();
        assert!(raw.struct_size as usize >= sys::MOQ_LOCATION_FILTER_CFG_V0_SIZE as usize);
        assert_eq!(raw.kind, sys::MOQ_LOCATION_FILTER_GROUP_END);
        assert_eq!(
            (raw.start_group, raw.start_object, raw.end_group),
            (4, 2, 9)
        );
        assert_eq!(
            GroupOrder::Descending.code(),
            sys::MOQ_GROUP_ORDER_DESCENDING
        );
    }
}
