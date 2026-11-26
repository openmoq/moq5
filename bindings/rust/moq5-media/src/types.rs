//! Value types of the media tier: what a track carries, how it is packaged,
//! stream access points, the companion conventions and time domains.

use crate::sys;

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
    /// A type from its `moq_media_type_t` value; `None` for zero, which the
    /// catalog model uses for "not derived".
    #[must_use]
    pub fn from_code(code: sys::moq_media_type_t) -> Option<MediaType> {
        match code {
            0 => None,
            sys::MOQ_MEDIA_TYPE_VIDEO => Some(MediaType::Video),
            sys::MOQ_MEDIA_TYPE_AUDIO => Some(MediaType::Audio),
            code => Some(MediaType::Other(code)),
        }
    }

    /// The `moq_media_type_t` value.
    #[must_use]
    pub fn code(self) -> sys::moq_media_type_t {
        match self {
            MediaType::Video => sys::MOQ_MEDIA_TYPE_VIDEO,
            MediaType::Audio => sys::MOQ_MEDIA_TYPE_AUDIO,
            MediaType::Other(code) => code,
        }
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
    /// MPEG-2 transport stream packets (draft-gregoire-moq-msfts). A sender
    /// adds such a track with its record ([`Mpeg2tsRecord`]).
    ///
    /// [`Mpeg2tsRecord`]: crate::Mpeg2tsRecord
    Mpeg2ts,
    /// LOCMAF (draft-einarsson-moq-locmaf-01): CMAF chunks with their moof
    /// boxes compacted against the track's CMAF Header and, within a group,
    /// against the chunk before. A track is declared with it; a receiver's
    /// objects of such a track are the canonical CMAF chunks the service
    /// rebuilt, so they report [`Packaging::Cmaf`]. A sender takes one CMAF
    /// chunk per write, with the CMAF Header as the track's init data.
    Locmaf,
    /// A packaging this crate does not know.
    Other(u32),
}

impl Packaging {
    /// A packaging from its `moq_media_packaging_t` value; `None` for zero,
    /// which the catalog model uses for "not derived".
    #[must_use]
    pub fn from_code(code: sys::moq_media_packaging_t) -> Option<Packaging> {
        match code {
            0 => None,
            sys::MOQ_MEDIA_PACKAGING_RAW => Some(Packaging::Raw),
            sys::MOQ_MEDIA_PACKAGING_CMAF => Some(Packaging::Cmaf),
            sys::MOQ_MEDIA_PACKAGING_MPEG2TS => Some(Packaging::Mpeg2ts),
            sys::MOQ_MEDIA_PACKAGING_LOCMAF => Some(Packaging::Locmaf),
            code => Some(Packaging::Other(code)),
        }
    }

    /// The `moq_media_packaging_t` value.
    #[must_use]
    pub fn code(self) -> sys::moq_media_packaging_t {
        match self {
            Packaging::Raw => sys::MOQ_MEDIA_PACKAGING_RAW,
            Packaging::Cmaf => sys::MOQ_MEDIA_PACKAGING_CMAF,
            Packaging::Mpeg2ts => sys::MOQ_MEDIA_PACKAGING_MPEG2TS,
            Packaging::Locmaf => sys::MOQ_MEDIA_PACKAGING_LOCMAF,
            Packaging::Other(code) => code,
        }
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
    /// A SAP type from its `moq_sap_type_t` value; a value outside the set
    /// reads as [`SapType::Unknown`].
    #[must_use]
    pub fn from_code(code: sys::moq_sap_type_t) -> SapType {
        match code {
            sys::MOQ_SAP_NONE => SapType::None,
            sys::MOQ_SAP_TYPE_1 => SapType::Type1,
            sys::MOQ_SAP_TYPE_2 => SapType::Type2,
            sys::MOQ_SAP_TYPE_3 => SapType::Type3,
            _ => SapType::Unknown,
        }
    }

    /// The `moq_sap_type_t` value.
    #[must_use]
    pub fn code(self) -> sys::moq_sap_type_t {
        match self {
            SapType::None => sys::MOQ_SAP_NONE,
            SapType::Type1 => sys::MOQ_SAP_TYPE_1,
            SapType::Type2 => sys::MOQ_SAP_TYPE_2,
            SapType::Type3 => sys::MOQ_SAP_TYPE_3,
            SapType::Unknown => sys::MOQ_SAP_UNKNOWN,
        }
    }
}

/// The media companion: which catalog and object-metadata conventions a
/// sender or receiver follows (`moq_media_companion_profile_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum CompanionProfile {
    /// Follow the negotiated draft: LEGACY on drafts 16/18, LOC-05 on
    /// draft 22.
    #[default]
    Auto,
    /// MSF-01 catalog with LOC-01 properties; requires drafts 16/18.
    LegacyMsf01Loc01,
    /// MSF-01 catalog with LOC-04 metadata; requires draft 22.
    D22Msf01Loc04Public,
    /// MSF-01 catalog with LOC-05 metadata (LOC-04 plus the Codec String);
    /// requires draft 22.
    D22Msf01Loc05Public,
    /// A profile this crate does not know.
    Other(u32),
}

impl CompanionProfile {
    /// A profile from its `MOQ_MEDIA_COMPANION_*` value, known or not.
    #[must_use]
    pub fn from_code(code: sys::moq_media_companion_profile_t) -> CompanionProfile {
        match code {
            sys::MOQ_MEDIA_COMPANION_AUTO => CompanionProfile::Auto,
            sys::MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01 => CompanionProfile::LegacyMsf01Loc01,
            sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC => {
                CompanionProfile::D22Msf01Loc04Public
            }
            sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC => {
                CompanionProfile::D22Msf01Loc05Public
            }
            code => CompanionProfile::Other(code),
        }
    }

    /// The `MOQ_MEDIA_COMPANION_*` value.
    #[must_use]
    pub fn code(self) -> sys::moq_media_companion_profile_t {
        match self {
            CompanionProfile::Auto => sys::MOQ_MEDIA_COMPANION_AUTO,
            CompanionProfile::LegacyMsf01Loc01 => sys::MOQ_MEDIA_COMPANION_LEGACY_MSF01_LOC01,
            CompanionProfile::D22Msf01Loc04Public => {
                sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC04_PUBLIC
            }
            CompanionProfile::D22Msf01Loc05Public => {
                sys::MOQ_MEDIA_COMPANION_D22_MSF01_LOC05_PUBLIC
            }
            CompanionProfile::Other(code) => code,
        }
    }
}

/// The MSF revision a catalog follows (`moq_msf_revision_t`, an open set).
/// An independent catalog's revision is its root `version` (MSF-01 and
/// MSF-02 5.1.1); a delta update follows the catalog it is applied to.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MsfRevision {
    /// draft-ietf-moq-msf-01. Zero, so an absent or zeroed field selects it.
    Msf01,
    /// draft-ietf-moq-msf-02.
    Msf02,
    /// A revision this crate does not know.
    Other(u32),
}

impl MsfRevision {
    /// A revision from its `MOQ_MSF_REVISION_*` value, known or not.
    #[must_use]
    pub fn from_code(code: sys::moq_msf_revision_t) -> MsfRevision {
        match code {
            sys::MOQ_MSF_REVISION_01 => MsfRevision::Msf01,
            sys::MOQ_MSF_REVISION_02 => MsfRevision::Msf02,
            code => MsfRevision::Other(code),
        }
    }

    /// The `MOQ_MSF_REVISION_*` value.
    #[must_use]
    pub fn code(self) -> sys::moq_msf_revision_t {
        match self {
            MsfRevision::Msf01 => sys::MOQ_MSF_REVISION_01,
            MsfRevision::Msf02 => sys::MOQ_MSF_REVISION_02,
            MsfRevision::Other(code) => code,
        }
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
    /// A domain from its `MOQ_MEDIA_TIME_DOMAIN_*` value, known or not.
    #[must_use]
    pub fn from_code(code: sys::moq_media_time_domain_t) -> TimeDomain {
        match code {
            sys::MOQ_MEDIA_TIME_DOMAIN_NONE => TimeDomain::None,
            sys::MOQ_MEDIA_TIME_DOMAIN_UNIX_US => TimeDomain::UnixMicros,
            sys::MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA => TimeDomain::ApplicationMedia,
            code => TimeDomain::Other(code),
        }
    }

    /// The `MOQ_MEDIA_TIME_DOMAIN_*` value.
    #[must_use]
    pub fn code(self) -> sys::moq_media_time_domain_t {
        match self {
            TimeDomain::None => sys::MOQ_MEDIA_TIME_DOMAIN_NONE,
            TimeDomain::UnixMicros => sys::MOQ_MEDIA_TIME_DOMAIN_UNIX_US,
            TimeDomain::ApplicationMedia => sys::MOQ_MEDIA_TIME_DOMAIN_APPLICATION_MEDIA,
            TimeDomain::Other(code) => code,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_sets_round_trip() {
        assert_eq!(Packaging::from_code(0), None);
        assert_eq!(Packaging::from_code(3), Some(Packaging::Mpeg2ts));
        assert_eq!(Packaging::from_code(77), Some(Packaging::Other(77)));
        assert_eq!(Packaging::Other(77).code(), 77);
        assert_eq!(MediaType::from_code(0), None);
        assert_eq!(MediaType::from_code(9), Some(MediaType::Other(9)));
        assert_eq!(
            MediaType::from_code(MediaType::Audio.code()),
            Some(MediaType::Audio)
        );
        assert_eq!(CompanionProfile::default(), CompanionProfile::Auto);
        assert_eq!(CompanionProfile::Auto.code(), sys::MOQ_MEDIA_COMPANION_AUTO);
        assert_eq!(CompanionProfile::from_code(77), CompanionProfile::Other(77));
        assert_eq!(TimeDomain::from_code(9), TimeDomain::Other(9));
        assert_eq!(
            TimeDomain::from_code(TimeDomain::UnixMicros.code()),
            TimeDomain::UnixMicros
        );
        assert_eq!(
            MsfRevision::from_code(sys::MOQ_MSF_REVISION_01),
            MsfRevision::Msf01
        );
        assert_eq!(
            MsfRevision::from_code(sys::MOQ_MSF_REVISION_02),
            MsfRevision::Msf02
        );
        assert_eq!(MsfRevision::Msf01.code(), 0, "zero selects MSF-01");
        assert_eq!(MsfRevision::Msf02.code(), sys::MOQ_MSF_REVISION_02);
        assert_eq!(MsfRevision::from_code(7), MsfRevision::Other(7));
        assert_eq!(MsfRevision::Other(7).code(), 7);
        assert_eq!(SapType::from_code(200), SapType::Unknown);
        for sap in [
            SapType::None,
            SapType::Type1,
            SapType::Type2,
            SapType::Type3,
            SapType::Unknown,
        ] {
            assert_eq!(SapType::from_code(sap.code()), sap);
        }
    }

    #[test]
    fn locmaf_is_a_named_packaging() {
        let locmaf =
            Packaging::from_code(sys::MOQ_MEDIA_PACKAGING_LOCMAF).expect("a nonzero packaging");
        assert!(
            !matches!(locmaf, Packaging::Other(_)),
            "{locmaf:?} is a named packaging"
        );
        assert_eq!(locmaf.code(), sys::MOQ_MEDIA_PACKAGING_LOCMAF);
        assert_eq!(locmaf, Packaging::Locmaf);
        assert_eq!(Packaging::from_code(5), Some(Packaging::Other(5)));
    }
}
