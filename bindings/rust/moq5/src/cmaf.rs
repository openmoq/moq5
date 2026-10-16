//! CMAF parsing: init segments (ftyp+moov) and media fragments (moof+mdat).
//!
//! Pure functions over byte slices: no session, no allocation inside libmoq.

use crate::error::{Error, Result};
use crate::sys;
use crate::types::{bytes, span, SapType};

/// The codec of a CMAF track.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum CodecKind {
    Unknown,
    Avc,
    Hevc,
    Av1,
    Aac,
    Opus,
    /// A kind this crate does not know yet.
    Other(u32),
}

impl CodecKind {
    fn from_raw(v: sys::moq_cmaf_codec_kind_t) -> CodecKind {
        match v {
            sys::MOQ_CMAF_CODEC_UNKNOWN => CodecKind::Unknown,
            sys::MOQ_CMAF_CODEC_AVC => CodecKind::Avc,
            sys::MOQ_CMAF_CODEC_HEVC => CodecKind::Hevc,
            sys::MOQ_CMAF_CODEC_AV1 => CodecKind::Av1,
            sys::MOQ_CMAF_CODEC_AAC => CodecKind::Aac,
            sys::MOQ_CMAF_CODEC_OPUS => CodecKind::Opus,
            v => CodecKind::Other(v),
        }
    }

    /// The `moq_cmaf_codec_kind_t` value.
    pub fn code(self) -> u32 {
        match self {
            CodecKind::Unknown => sys::MOQ_CMAF_CODEC_UNKNOWN,
            CodecKind::Avc => sys::MOQ_CMAF_CODEC_AVC,
            CodecKind::Hevc => sys::MOQ_CMAF_CODEC_HEVC,
            CodecKind::Av1 => sys::MOQ_CMAF_CODEC_AV1,
            CodecKind::Aac => sys::MOQ_CMAF_CODEC_AAC,
            CodecKind::Opus => sys::MOQ_CMAF_CODEC_OPUS,
            CodecKind::Other(v) => v,
        }
    }
}

/// CENC protection parameters from an encrypted sample entry (CMSF 4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cenc<'a> {
    /// The schm scheme 4CC: `cenc` or `cbcs`.
    pub scheme: &'a [u8],
    /// tenc default_KID, 16 raw bytes.
    pub default_kid: &'a [u8],
    /// tenc default_isProtected.
    pub default_is_protected: u8,
    /// tenc default_Per_Sample_IV_Size.
    pub default_per_sample_iv_size: u8,
}

/// A view of a parsed init segment's first track. Its byte fields borrow
/// from whatever owns the segment.
#[derive(Clone, Copy)]
pub struct InitInfo<'a>(&'a sys::moq_cmaf_init_info_t);

impl<'a> InitInfo<'a> {
    /// View a raw init info.
    ///
    /// # Safety
    /// Every span in `raw` must be valid for `'a`.
    pub unsafe fn from_raw(raw: &'a sys::moq_cmaf_init_info_t) -> InitInfo<'a> {
        InitInfo(raw)
    }

    /// The codec.
    pub fn codec_kind(&self) -> CodecKind {
        CodecKind::from_raw(self.0.codec_kind)
    }
    /// Media timescale (mdhd).
    pub fn timescale(&self) -> u32 {
        self.0.timescale
    }
    /// Video width (0 for audio).
    pub fn width(&self) -> u32 {
        self.0.width
    }
    /// Video height (0 for audio).
    pub fn height(&self) -> u32 {
        self.0.height
    }
    /// Audio sample rate (0 for video).
    pub fn samplerate(&self) -> u32 {
        self.0.samplerate
    }
    /// Audio channel count (0 for video).
    pub fn channel_count(&self) -> u32 {
        self.0.channel_count
    }
    /// Decoder extradata: avcC, hvcC, AudioSpecificConfig, ...
    pub fn codec_config(&self) -> &'a [u8] {
        unsafe { bytes(self.0.codec_config) }
    }
    /// tkhd track_ID (0 if absent).
    pub fn track_id(&self) -> u32 {
        self.0.track_id
    }
    /// The CENC parameters when the track is encrypted.
    pub fn cenc(&self) -> Option<Cenc<'a>> {
        self.0.has_cenc.then(|| Cenc {
            scheme: unsafe { bytes(self.0.scheme) },
            default_kid: unsafe { bytes(self.0.default_kid) },
            default_is_protected: self.0.default_is_protected,
            default_per_sample_iv_size: self.0.default_per_sample_iv_size,
        })
    }
    /// The raw struct.
    pub fn raw(&self) -> &'a sys::moq_cmaf_init_info_t {
        self.0
    }
}

impl std::fmt::Debug for InitInfo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitInfo")
            .field("codec_kind", &self.codec_kind())
            .field("timescale", &self.timescale())
            .field("width", &self.width())
            .field("height", &self.height())
            .field("samplerate", &self.samplerate())
            .field("channel_count", &self.channel_count())
            .field("track_id", &self.track_id())
            .field("cenc", &self.cenc())
            .finish()
    }
}

/// An init segment (ftyp + moov) together with its parsed first track.
pub struct CmafInit {
    data: Box<[u8]>,
    raw: sys::moq_cmaf_init_info_t,
}

// SAFETY: the info's spans point into `data`, a heap buffer that moves with
// it and is never mutated.
unsafe impl Send for CmafInit {}
unsafe impl Sync for CmafInit {}

impl CmafInit {
    /// Parse `data`, keeping it.
    pub fn parse(data: impl Into<Box<[u8]>>) -> Result<CmafInit> {
        let data = data.into();
        let mut raw = sys::moq_cmaf_init_info_t::default();
        unsafe {
            sys::moq_cmaf_init_info_init(&mut raw);
            match sys::moq_cmaf_parse_init(span(&data), &mut raw) {
                sys::MOQ_OK => Ok(CmafInit { data, raw }),
                rc => Err(Error::from_code(rc)),
            }
        }
    }

    /// The segment bytes.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// The parsed first track.
    pub fn info(&self) -> InitInfo<'_> {
        InitInfo(&self.raw)
    }
}

impl std::fmt::Debug for CmafInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CmafInit")
            .field("len", &self.data.len())
            .field("info", &self.info())
            .finish()
    }
}

/// One entry of a fragment's sample table (trun).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct Sample {
    pub duration: u32,
    pub size: u32,
    pub flags: u32,
    pub composition_offset: i32,
}

impl Sample {
    /// `sample_is_non_sync_sample` is clear.
    pub fn is_sync(&self) -> bool {
        self.flags & 0x0001_0000 == 0
    }
}

/// A parsed media fragment. When the object holds several CMAF chunks, the
/// table, decode time, track and mdat are the first chunk's.
#[derive(Clone, Debug)]
pub struct Fragment<'a> {
    /// tfdt baseMediaDecodeTime.
    pub base_decode_time: Option<u64>,
    /// tfhd default sample duration.
    pub default_sample_duration: u32,
    /// tfhd default sample size.
    pub default_sample_size: u32,
    /// tfhd default sample flags.
    pub default_sample_flags: u32,
    /// The sample table.
    pub samples: Vec<Sample>,
    /// The first chunk's mdat payload.
    pub mdat: &'a [u8],
    /// tfhd track_ID (0 if none).
    pub track_id: u32,
    /// moof+mdat chunks in the object.
    pub chunk_count: u32,
}

impl Fragment<'_> {
    /// The first sample's flags, or the tfhd default when the table is empty.
    pub fn first_sample_flags(&self) -> u32 {
        self.samples
            .first()
            .map_or(self.default_sample_flags, |s| s.flags)
    }
}

/// Parse a moof+mdat fragment, sizing the sample table to fit.
pub fn parse_fragment(fragment: &[u8]) -> Result<Fragment<'_>> {
    let mut samples = vec![sys::moq_cmaf_sample_t::default(); 64];
    let mut fi = sys::moq_cmaf_fragment_info_t::default();
    let parse = |samples: &mut Vec<sys::moq_cmaf_sample_t>,
                 fi: &mut sys::moq_cmaf_fragment_info_t| unsafe {
        sys::moq_cmaf_fragment_info_init(fi, samples.as_mut_ptr(), samples.len());
        sys::moq_cmaf_parse_fragment(span(fragment), fi)
    };
    let mut rc = parse(&mut samples, &mut fi);
    if rc == sys::MOQ_ERR_BUFFER {
        // A structurally valid fragment with more samples: the count is
        // trusted and fragment-bounded, so grow once and reparse.
        samples = vec![sys::moq_cmaf_sample_t::default(); fi.sample_count];
        rc = parse(&mut samples, &mut fi);
    }
    if rc != sys::MOQ_OK {
        return Err(Error::from_code(rc));
    }
    samples.truncate(fi.sample_count);
    Ok(Fragment {
        base_decode_time: fi.has_base_decode_time.then_some(fi.base_decode_time),
        default_sample_duration: fi.default_sample_duration,
        default_sample_size: fi.default_sample_size,
        default_sample_flags: fi.default_sample_flags,
        samples: samples
            .iter()
            .map(|s| Sample {
                duration: s.duration,
                size: s.size,
                flags: s.flags,
                composition_offset: s.composition_offset,
            })
            .collect(),
        // SAFETY: borrowed from `fragment`.
        mdat: unsafe { bytes(fi.mdat) },
        track_id: fi.track_id,
        chunk_count: fi.chunk_count,
    })
}

/// Why an object failed CMSF 3.3 packaging.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Validity {
    Ok,
    NoChunk,
    MissingMfhd,
    MultiTrack,
    TrackIdMismatch,
    Malformed,
    NoSamples,
    Other(u32),
}

/// The result of [`validate_object`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectReport {
    /// moof boxes seen.
    pub chunk_count: u32,
    /// The first tfhd track_ID (0 if none).
    pub track_id: u32,
    /// The first sample of the first chunk is a sync sample.
    pub starts_with_sync: bool,
    /// The first sample's SAP type.
    pub sap_type: SapType,
    /// Passes CMSF 3.3.
    pub valid: bool,
    /// The first failure, [`Validity::Ok`] when valid.
    pub reason: Validity,
}

/// Check one object payload against CMSF 3.3 object packaging, and, with
/// `init`, its track_ID against the init segment's.
pub fn validate_object(init: Option<InitInfo<'_>>, object: &[u8]) -> Result<ObjectReport> {
    let mut r = sys::moq_cmaf_object_report_t::default();
    let init = init.map_or(std::ptr::null(), |i| i.0 as *const _);
    let rc = unsafe {
        sys::moq_cmaf_object_report_init(&mut r);
        sys::moq_cmaf_validate_object(init, span(object), &mut r)
    };
    if rc != sys::MOQ_OK && rc != sys::MOQ_ERR_PROTO {
        return Err(Error::from_code(rc));
    }
    Ok(ObjectReport {
        chunk_count: r.chunk_count,
        track_id: r.track_id,
        starts_with_sync: r.starts_with_sync,
        sap_type: SapType::from_raw(r.sap_type),
        valid: r.valid,
        reason: match r.reason {
            sys::MOQ_CMAF_OK => Validity::Ok,
            sys::MOQ_CMAF_ERR_NO_CHUNK => Validity::NoChunk,
            sys::MOQ_CMAF_ERR_MISSING_MFHD => Validity::MissingMfhd,
            sys::MOQ_CMAF_ERR_MULTI_TRACK => Validity::MultiTrack,
            sys::MOQ_CMAF_ERR_TRACK_ID_MISMATCH => Validity::TrackIdMismatch,
            sys::MOQ_CMAF_ERR_MALFORMED => Validity::Malformed,
            sys::MOQ_CMAF_ERR_NO_SAMPLES => Validity::NoSamples,
            v => Validity::Other(v),
        },
    })
}

/// Classify a sample's SAP type from its ISO-BMFF flags alone. A sync sample
/// reports [`SapType::Unknown`]: it is SAP 1 or 2, but flags cannot tell
/// which.
pub fn sap_from_sample_flags(flags: u32) -> SapType {
    SapType::from_raw(unsafe { sys::moq_cmaf_sap_from_sample_flags(flags) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_garbage() {
        assert_eq!(CmafInit::parse(vec![0u8; 16]).unwrap_err(), Error::PROTO);
        assert!(parse_fragment(&[0u8; 16]).is_err());
        let r = validate_object(None, &[0u8; 16]).unwrap();
        assert!(!r.valid);
        assert_ne!(r.reason, Validity::Ok);
    }

    #[test]
    fn sap_flags() {
        assert_eq!(sap_from_sample_flags(0x0101_0000), SapType::None);
        assert_eq!(sap_from_sample_flags(0x0200_0000), SapType::Unknown);
        assert!(Sample {
            flags: 0x0200_0000,
            ..Default::default()
        }
        .is_sync());
    }
}
