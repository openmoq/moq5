use std::time::Duration;

use crate::sys;

/// Borrow `s` as a libmoq byte span. An empty slice becomes the null span,
/// which libmoq reads as "absent".
pub(crate) fn span(s: &[u8]) -> sys::moq_bytes_t {
    if s.is_empty() {
        sys::moq_bytes_t::default()
    } else {
        sys::moq_bytes_t {
            data: s.as_ptr(),
            len: s.len(),
        }
    }
}

/// View a libmoq byte span as a slice (empty for a null span).
///
/// # Safety
/// `b` must describe memory valid for `'a`.
pub(crate) unsafe fn bytes<'a>(b: sys::moq_bytes_t) -> &'a [u8] {
    if b.data.is_null() || b.len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(b.data, b.len)
    }
}

/// View a libmoq (pointer, count) array as a slice.
///
/// # Safety
/// `ptr..ptr+len` must be valid for `'a`.
pub(crate) unsafe fn array<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if ptr.is_null() || len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(ptr, len)
    }
}

/// A timeout in libmoq's microseconds, saturating.
pub(crate) fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Outcome of a blocking wait that did not fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wait {
    /// The awaited condition holds, or the object woke (`MOQ_OK`).
    Ready,
    /// The timeout elapsed first (`MOQ_DONE`).
    TimedOut,
}

pub(crate) fn wait_result(rc: sys::moq_result_t) -> crate::Result<Wait> {
    match rc {
        sys::MOQ_OK => Ok(Wait::Ready),
        sys::MOQ_DONE => Ok(Wait::TimedOut),
        rc => Err(crate::Error::from_code(rc)),
    }
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
    pub const fn from_draft(draft: u32) -> Version {
        Version(draft)
    }

    /// The draft number.
    pub const fn draft(self) -> u32 {
        self.0
    }

    pub(crate) fn raw(self) -> sys::moq_version_t {
        self.0
    }
}

/// What a track carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum MediaType {
    /// Video.
    Video,
    /// Audio.
    Audio,
}

impl MediaType {
    pub(crate) fn raw(self) -> sys::moq_media_type_t {
        match self {
            MediaType::Video => sys::MOQ_MEDIA_TYPE_VIDEO,
            MediaType::Audio => sys::MOQ_MEDIA_TYPE_AUDIO,
        }
    }

    pub(crate) fn from_raw(v: sys::moq_media_type_t) -> Option<MediaType> {
        match v {
            sys::MOQ_MEDIA_TYPE_VIDEO => Some(MediaType::Video),
            sys::MOQ_MEDIA_TYPE_AUDIO => Some(MediaType::Audio),
            _ => None,
        }
    }
}

/// How a track's media is packaged in its objects.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Packaging {
    /// LOC: one access unit per object, timing in LOC properties.
    Loc,
    /// CMAF: one moof+mdat fragment per object.
    Cmaf,
}

impl Packaging {
    pub(crate) fn raw(self) -> sys::moq_media_packaging_t {
        match self {
            Packaging::Loc => sys::MOQ_MEDIA_PACKAGING_RAW,
            Packaging::Cmaf => sys::MOQ_MEDIA_PACKAGING_CMAF,
        }
    }

    pub(crate) fn from_raw(v: sys::moq_media_packaging_t) -> Option<Packaging> {
        match v {
            sys::MOQ_MEDIA_PACKAGING_RAW => Some(Packaging::Loc),
            sys::MOQ_MEDIA_PACKAGING_CMAF => Some(Packaging::Cmaf),
            _ => None,
        }
    }
}

/// A CMSF Stream Access Point type (CMSF 3.6).
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

    pub(crate) fn from_raw(v: sys::moq_sap_type_t) -> SapType {
        match v {
            sys::MOQ_SAP_NONE => SapType::None,
            sys::MOQ_SAP_TYPE_1 => SapType::Type1,
            sys::MOQ_SAP_TYPE_2 => SapType::Type2,
            sys::MOQ_SAP_TYPE_3 => SapType::Type3,
            _ => SapType::Unknown,
        }
    }
}
