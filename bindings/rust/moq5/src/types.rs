//! Value types shared by the endpoint, sender and receiver.

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

/// Outcome of a blocking wait that did not fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitResult {
    /// The awaited level holds, or the endpoint woke (`MOQ_OK`).
    Ready,
    /// The timeout elapsed first (`MOQ_DONE`).
    TimedOut,
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

    pub(crate) fn raw(self) -> sys::moq_version_t {
        self.0
    }
}

/// A MoQ Location: a group and an object within it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Location {
    /// The group ID (zero is valid).
    pub group_id: u64,
    /// The object ID within the group (zero is valid).
    pub object_id: u64,
}

/// What a track carries (`moq_media_type_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MediaType {
    /// Video.
    Video,
    /// Audio.
    Audio,
    /// A type this crate does not know.
    Other(u32),
}

impl MediaType {
    pub(crate) fn raw(self) -> sys::moq_media_type_t {
        match self {
            MediaType::Video => sys::MOQ_MEDIA_TYPE_VIDEO,
            MediaType::Audio => sys::MOQ_MEDIA_TYPE_AUDIO,
            MediaType::Other(code) => code,
        }
    }

    /// `None` for zero, which the catalog model uses for "not derived".
    pub(crate) fn from_raw(code: sys::moq_media_type_t) -> Option<MediaType> {
        match code {
            0 => None,
            sys::MOQ_MEDIA_TYPE_VIDEO => Some(MediaType::Video),
            sys::MOQ_MEDIA_TYPE_AUDIO => Some(MediaType::Audio),
            code => Some(MediaType::Other(code)),
        }
    }

    /// The `moq_media_type_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        self.raw()
    }
}

/// How a track's media is packaged in its objects (`moq_media_packaging_t`,
/// an open set).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Packaging {
    /// RAW/LOC: one access unit per object, timing in the object properties.
    Raw,
    /// CMAF: one moof+mdat fragment per object.
    Cmaf,
    /// MPEG-2 transport stream packets (draft-gregoire-moq-msfts). A track
    /// is added with its record ([`SendTrackConfig::mpeg2ts`]).
    ///
    /// [`SendTrackConfig::mpeg2ts`]: crate::SendTrackConfig::mpeg2ts
    Mpeg2ts,
    /// A packaging this crate does not know.
    Other(u32),
}

impl Packaging {
    pub(crate) fn raw(self) -> sys::moq_media_packaging_t {
        match self {
            Packaging::Raw => sys::MOQ_MEDIA_PACKAGING_RAW,
            Packaging::Cmaf => sys::MOQ_MEDIA_PACKAGING_CMAF,
            Packaging::Mpeg2ts => sys::MOQ_MEDIA_PACKAGING_MPEG2TS,
            Packaging::Other(code) => code,
        }
    }

    /// `None` for zero, which the catalog model uses for "not derived".
    pub(crate) fn from_raw(code: sys::moq_media_packaging_t) -> Option<Packaging> {
        match code {
            0 => None,
            sys::MOQ_MEDIA_PACKAGING_RAW => Some(Packaging::Raw),
            sys::MOQ_MEDIA_PACKAGING_CMAF => Some(Packaging::Cmaf),
            sys::MOQ_MEDIA_PACKAGING_MPEG2TS => Some(Packaging::Mpeg2ts),
            code => Some(Packaging::Other(code)),
        }
    }

    /// The `moq_media_packaging_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        self.raw()
    }
}

/// A CMSF Stream Access Point type (CMSF 3.6, `moq_sap_type_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SapType {
    /// Known not to start with a SAP.
    None,
    /// SAP type 1.
    Type1,
    /// SAP type 2.
    Type2,
    /// SAP type 3 (open GOP).
    Type3,
    /// A SAP whose exact type could not be classified. Only ever reported;
    /// libmoq rejects it as a declaration.
    Unknown,
}

impl SapType {
    pub(crate) fn raw(self) -> sys::moq_sap_type_t {
        match self {
            SapType::None => sys::MOQ_SAP_NONE,
            SapType::Type1 => sys::MOQ_SAP_TYPE_1,
            SapType::Type2 => sys::MOQ_SAP_TYPE_2,
            SapType::Type3 => sys::MOQ_SAP_TYPE_3,
            SapType::Unknown => sys::MOQ_SAP_UNKNOWN,
        }
    }

    pub(crate) fn from_raw(code: sys::moq_sap_type_t) -> SapType {
        match code {
            sys::MOQ_SAP_NONE => SapType::None,
            sys::MOQ_SAP_TYPE_1 => SapType::Type1,
            sys::MOQ_SAP_TYPE_2 => SapType::Type2,
            sys::MOQ_SAP_TYPE_3 => SapType::Type3,
            _ => SapType::Unknown,
        }
    }
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

/// The media companion: which catalog and object-metadata conventions a
/// sender or receiver follows (`moq_media_companion_profile_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CompanionProfile {
    /// Follow the negotiated draft: LEGACY on drafts 16/18, D22 on draft 22.
    #[default]
    Auto,
    /// MSF-01 catalog with LOC-01 properties; requires drafts 16/18.
    LegacyMsf01Loc01,
    /// MSF-01 catalog with LOC-04 metadata; requires draft 22.
    D22Msf01Loc04Public,
    /// A profile this crate does not know.
    Other(u32),
}

impl CompanionProfile {
    pub(crate) fn raw(self) -> sys::moq_media_companion_profile_t {
        match self {
            CompanionProfile::Auto => sys::MOQ_MEDIA_COMPANION_AUTO,
            CompanionProfile::LegacyMsf01Loc01 => sys::MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
            CompanionProfile::D22Msf01Loc04Public => {
                sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
            }
            CompanionProfile::Other(code) => code,
        }
    }

    pub(crate) fn from_raw(code: sys::moq_media_companion_profile_t) -> CompanionProfile {
        match code {
            sys::MOQ_MEDIA_COMPANION_AUTO => CompanionProfile::Auto,
            sys::MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 => CompanionProfile::LegacyMsf01Loc01,
            sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC => {
                CompanionProfile::D22Msf01Loc04Public
            }
            code => CompanionProfile::Other(code),
        }
    }

    /// The `MOQ_MEDIA_COMPANION_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        self.raw()
    }
}

/// The clock a timestamp is in (`moq_media_time_domain_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TimeDomain {
    /// No clock is known.
    None,
    /// Microseconds since the Unix epoch: a capture clock.
    UnixMicros,
    /// The application's own media time (LOC with a Timescale), with an
    /// epoch only the application knows.
    ApplicationMedia,
    /// A domain this crate does not know.
    Other(u32),
}

impl TimeDomain {
    pub(crate) fn from_raw(code: sys::moq_media_time_domain_t) -> TimeDomain {
        match code {
            sys::MOQ_MEDIA_TIME_DOMAIN_NONE => TimeDomain::None,
            sys::MOQ_MEDIA_TIME_DOMAIN_UNIX_US => TimeDomain::UnixMicros,
            sys::MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA => TimeDomain::ApplicationMedia,
            code => TimeDomain::Other(code),
        }
    }

    /// The `MOQ_MEDIA_TIME_DOMAIN_*` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            TimeDomain::None => sys::MOQ_MEDIA_TIME_DOMAIN_NONE,
            TimeDomain::UnixMicros => sys::MOQ_MEDIA_TIME_DOMAIN_UNIX_US,
            TimeDomain::ApplicationMedia => sys::MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA,
            TimeDomain::Other(code) => code,
        }
    }
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
    /// library's initializer.
    pub(crate) fn raw(self) -> sys::moq_location_filter_cfg_t {
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

/// A MoQ object status (`moq_object_status_t`, an open set).
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
    pub(crate) fn from_raw(code: sys::moq_object_status_t) -> ObjectStatus {
        match u32::from(code) {
            sys::MOQ_OBJECT_STATUS_NORMAL => ObjectStatus::Normal,
            sys::MOQ_OBJECT_STATUS_END_OF_GROUP => ObjectStatus::EndOfGroup,
            sys::MOQ_OBJECT_STATUS_END_OF_TRACK => ObjectStatus::EndOfTrack,
            _ => ObjectStatus::Other(code),
        }
    }

    /// The `MOQ_OBJECT_STATUS_*` value.
    #[must_use]
    pub fn code(self) -> u8 {
        match self {
            ObjectStatus::Normal => sys::MOQ_OBJECT_STATUS_NORMAL as u8,
            ObjectStatus::EndOfGroup => sys::MOQ_OBJECT_STATUS_END_OF_GROUP as u8,
            ObjectStatus::EndOfTrack => sys::MOQ_OBJECT_STATUS_END_OF_TRACK as u8,
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
    fn open_sets_round_trip() {
        assert_eq!(Packaging::from_raw(0), None);
        assert_eq!(Packaging::from_raw(3), Some(Packaging::Mpeg2ts));
        assert_eq!(Packaging::from_raw(77), Some(Packaging::Other(77)));
        assert_eq!(Packaging::Other(77).code(), 77);
        assert_eq!(MediaType::from_raw(0), None);
        assert_eq!(MediaType::from_raw(9), Some(MediaType::Other(9)));
        assert_eq!(DeliveryOrigin::from_raw(2), DeliveryOrigin::Fill);
        assert_eq!(DeliveryOrigin::from_raw(9).code(), 9);
        assert_eq!(CompanionProfile::default(), CompanionProfile::Auto);
        assert_eq!(CompanionProfile::Auto.code(), sys::MOQ_MEDIA_COMPANION_AUTO);
        assert_eq!(ObjectStatus::from_raw(3), ObjectStatus::EndOfGroup);
        assert_eq!(ObjectStatus::from_raw(9), ObjectStatus::Other(9));
        assert!(ObjectStatus::EndOfTrack.is_status_only());
        assert!(!ObjectStatus::Normal.is_status_only());
        assert_eq!(SapType::from_raw(200), SapType::Unknown);
        assert!(
            Location::default()
                < Location {
                    group_id: 0,
                    object_id: 1
                }
        );
    }
}
