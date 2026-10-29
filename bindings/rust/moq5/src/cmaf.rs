//! CMAF parsing: init segments (ftyp+moov) and media fragments (moof+mdat).
//!
//! Pure functions over byte slices: no session, no allocation inside libmoq.

use std::{fmt, mem, ptr};

use crate::error::{Error, Result};
use crate::types::SapType;
use crate::{ffi, sys};

/// The codec of a CMAF track (`moq_cmaf_codec_kind_t`).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[allow(missing_docs)]
pub enum CodecKind {
    Unknown,
    Avc,
    Hevc,
    Av1,
    Aac,
    Opus,
    /// A kind this crate does not know.
    Other(u32),
}

impl CodecKind {
    fn from_raw(code: sys::moq_cmaf_codec_kind_t) -> CodecKind {
        match code {
            sys::MOQ_CMAF_CODEC_UNKNOWN => CodecKind::Unknown,
            sys::MOQ_CMAF_CODEC_AVC => CodecKind::Avc,
            sys::MOQ_CMAF_CODEC_HEVC => CodecKind::Hevc,
            sys::MOQ_CMAF_CODEC_AV1 => CodecKind::Av1,
            sys::MOQ_CMAF_CODEC_AAC => CodecKind::Aac,
            sys::MOQ_CMAF_CODEC_OPUS => CodecKind::Opus,
            code => CodecKind::Other(code),
        }
    }

    /// The `moq_cmaf_codec_kind_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        match self {
            CodecKind::Unknown => sys::MOQ_CMAF_CODEC_UNKNOWN,
            CodecKind::Avc => sys::MOQ_CMAF_CODEC_AVC,
            CodecKind::Hevc => sys::MOQ_CMAF_CODEC_HEVC,
            CodecKind::Av1 => sys::MOQ_CMAF_CODEC_AV1,
            CodecKind::Aac => sys::MOQ_CMAF_CODEC_AAC,
            CodecKind::Opus => sys::MOQ_CMAF_CODEC_OPUS,
            CodecKind::Other(code) => code,
        }
    }
}

/// CENC protection parameters from an encrypted sample entry (CMSF 4.2).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cenc<'segment> {
    /// The schm scheme 4CC: `cenc` or `cbcs`.
    pub scheme: &'segment [u8],
    /// tenc default_KID, 16 raw bytes.
    pub default_kid: &'segment [u8],
    /// tenc default_isProtected.
    pub default_is_protected: u8,
    /// tenc default_Per_Sample_IV_Size.
    pub default_per_sample_iv_size: u8,
}

/// A view of a parsed init segment's first track. Its byte fields borrow
/// from whatever owns the segment.
#[derive(Clone, Copy)]
pub struct InitInfo<'segment>(&'segment sys::moq_cmaf_init_info_t);

impl<'segment> InitInfo<'segment> {
    /// View a raw init info.
    ///
    /// # Safety
    /// Every span in `raw` must be valid for `'segment`.
    pub unsafe fn from_raw(raw: &'segment sys::moq_cmaf_init_info_t) -> InitInfo<'segment> {
        InitInfo(raw)
    }

    /// The codec.
    #[must_use]
    pub fn codec_kind(&self) -> CodecKind {
        CodecKind::from_raw(self.0.codec_kind)
    }
    /// Media timescale (mdhd).
    #[must_use]
    pub fn timescale(&self) -> u32 {
        self.0.timescale
    }
    /// Video width (0 for audio).
    #[must_use]
    pub fn width(&self) -> u32 {
        self.0.width
    }
    /// Video height (0 for audio).
    #[must_use]
    pub fn height(&self) -> u32 {
        self.0.height
    }
    /// Audio sample rate (0 for video).
    #[must_use]
    pub fn samplerate(&self) -> u32 {
        self.0.samplerate
    }
    /// Audio channel count (0 for video).
    #[must_use]
    pub fn channel_count(&self) -> u32 {
        self.0.channel_count
    }
    /// Decoder extradata: avcC, hvcC, AudioSpecificConfig, ...
    #[must_use]
    pub fn codec_config(&self) -> &'segment [u8] {
        // SAFETY: valid for 'segment by from_raw's contract.
        unsafe { ffi::bytes(self.0.codec_config) }
    }
    /// tkhd track_ID (0 if absent).
    #[must_use]
    pub fn track_id(&self) -> u32 {
        self.0.track_id
    }
    /// The CENC parameters when the track is encrypted.
    #[must_use]
    pub fn cenc(&self) -> Option<Cenc<'segment>> {
        if !self.0.has_cenc {
            return None;
        }
        Some(Cenc {
            // SAFETY: valid for 'segment by from_raw's contract.
            scheme: unsafe { ffi::bytes(self.0.scheme) },
            // SAFETY: as above.
            default_kid: unsafe { ffi::bytes(self.0.default_kid) },
            default_is_protected: self.0.default_is_protected,
            default_per_sample_iv_size: self.0.default_per_sample_iv_size,
        })
    }
    /// The sample entry's format as its four bytes: a clear entry's own
    /// type (`avc1`, `avc3`, `hvc1`, `hev1`, ...), or an encrypted entry's
    /// `frma` original format, never `encv`/`enca`. Any value, including
    /// zero, is reported as read; `None` when the library did not report it.
    #[must_use]
    pub fn sample_entry(&self) -> Option<[u8; 4]> {
        let end = ffi::field_end!(sys::moq_cmaf_init_info_t, sample_entry, u32);
        ffi::covers(self.0.struct_size, end).then(|| self.0.sample_entry.to_be_bytes())
    }
    /// The raw struct.
    #[must_use]
    pub fn raw(&self) -> &'segment sys::moq_cmaf_init_info_t {
        self.0
    }
}

// The parsed values, not the raw spans a derive would print.
impl fmt::Debug for InitInfo<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InitInfo")
            .field("codec_kind", &self.codec_kind())
            .field("timescale", &self.timescale())
            .field("width", &self.width())
            .field("height", &self.height())
            .field("samplerate", &self.samplerate())
            .field("channel_count", &self.channel_count())
            .field("track_id", &self.track_id())
            .field("cenc", &self.cenc())
            .field("sample_entry", &self.sample_entry())
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
        let size = mem::size_of_val(&raw);
        // SAFETY: raw is a writable record of the size passed; the span
        // borrows `data` for the call and the parsed spans point into it,
        // which the result keeps.
        ffi::check(unsafe { sys::moq_cmaf_parse_init_sized(ffi::span(&data), &mut raw, size) })?;
        ffi::check_stamp(raw.struct_size, sys::MOQ_CMAF_INIT_INFO_V0_SIZE, size)?;
        Ok(CmafInit { data, raw })
    }

    /// The segment bytes.
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// The parsed first track.
    #[must_use]
    pub fn info(&self) -> InitInfo<'_> {
        InitInfo(&self.raw)
    }
}

// The size and the parsed track, not the segment bytes.
impl fmt::Debug for CmafInit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CmafInit")
            .field("len", &self.data.len())
            .field("info", &self.info())
            .finish()
    }
}

/// One entry of a fragment's sample table (trun). The layout is
/// `moq_cmaf_sample_t`'s, so a received object's table is borrowed, not
/// copied.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub struct CmafSample {
    pub duration: u32,
    pub size: u32,
    pub flags: u32,
    pub composition_offset: i32,
}

const _: () = {
    assert!(mem::size_of::<CmafSample>() == mem::size_of::<sys::moq_cmaf_sample_t>());
    assert!(mem::align_of::<CmafSample>() == mem::align_of::<sys::moq_cmaf_sample_t>());
    assert!(
        mem::offset_of!(CmafSample, duration) == mem::offset_of!(sys::moq_cmaf_sample_t, duration)
    );
    assert!(mem::offset_of!(CmafSample, size) == mem::offset_of!(sys::moq_cmaf_sample_t, size));
    assert!(mem::offset_of!(CmafSample, flags) == mem::offset_of!(sys::moq_cmaf_sample_t, flags));
    assert!(
        mem::offset_of!(CmafSample, composition_offset)
            == mem::offset_of!(sys::moq_cmaf_sample_t, composition_offset)
    );
};

impl CmafSample {
    /// `sample_is_non_sync_sample` is clear.
    #[must_use]
    pub fn is_sync(&self) -> bool {
        self.flags & 0x0001_0000 == 0
    }

    /// View a C sample table.
    ///
    /// # Safety
    /// `ptr..ptr+len` must be valid for `'a`.
    pub(crate) unsafe fn table<'a>(
        ptr: *const sys::moq_cmaf_sample_t,
        len: usize,
    ) -> &'a [CmafSample] {
        // SAFETY: the caller's contract; the layouts are identical (asserted
        // above).
        unsafe { ffi::array(ptr as *const CmafSample, len) }
    }
}

/// A parsed media fragment. When the object holds several CMAF chunks, the
/// table, decode time, track and mdat are the first chunk's.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct Fragment<'fragment> {
    /// tfdt baseMediaDecodeTime.
    pub base_decode_time: Option<u64>,
    /// tfhd default sample duration.
    pub default_sample_duration: u32,
    /// tfhd default sample size.
    pub default_sample_size: u32,
    /// tfhd default sample flags.
    pub default_sample_flags: u32,
    /// The sample table.
    pub samples: Vec<CmafSample>,
    /// The first chunk's mdat payload.
    pub mdat: &'fragment [u8],
    /// tfhd track_ID (0 if none).
    pub track_id: u32,
    /// moof+mdat chunks in the object.
    pub chunk_count: u32,
}

impl Fragment<'_> {
    /// The first sample's flags, or the tfhd default when the table is empty.
    #[must_use]
    pub fn first_sample_flags(&self) -> u32 {
        self.samples
            .first()
            .map_or(self.default_sample_flags, |sample| sample.flags)
    }
}

/// Parse a moof+mdat fragment, sizing the sample table to fit.
pub fn parse_fragment(fragment: &[u8]) -> Result<Fragment<'_>> {
    let mut samples = vec![CmafSample::default(); 64];
    let mut info = sys::moq_cmaf_fragment_info_t::default();
    let parse = |samples: &mut Vec<CmafSample>, info: &mut sys::moq_cmaf_fragment_info_t| {
        // SAFETY: the table is writable for its length and has the C
        // layout; the span borrows `fragment` for the call.
        unsafe {
            sys::moq_cmaf_fragment_info_init(
                info,
                samples.as_mut_ptr() as *mut sys::moq_cmaf_sample_t,
                samples.len(),
            );
            sys::moq_cmaf_parse_fragment(ffi::span(fragment), info)
        }
    };
    let mut code = parse(&mut samples, &mut info);
    if code == sys::MOQ_ERR_BUFFER {
        // A structurally valid fragment with more samples: the count is
        // trusted and fragment-bounded, so grow once and reparse.
        samples = vec![CmafSample::default(); info.sample_count];
        code = parse(&mut samples, &mut info);
    }
    ffi::check(code)?;
    samples.truncate(info.sample_count);
    Ok(Fragment {
        base_decode_time: if info.has_base_decode_time {
            Some(info.base_decode_time)
        } else {
            None
        },
        default_sample_duration: info.default_sample_duration,
        default_sample_size: info.default_sample_size,
        default_sample_flags: info.default_sample_flags,
        samples,
        // SAFETY: borrowed from `fragment`, which the result borrows.
        mdat: unsafe { ffi::bytes(info.mdat) },
        track_id: info.track_id,
        chunk_count: info.chunk_count,
    })
}

/// Why an object failed CMSF 3.3 packaging (`moq_cmaf_validity_t`).
#[non_exhaustive]
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
#[non_exhaustive]
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
/// `init`, its track_ID against the init segment's. A failing object is an
/// `Ok` report with `valid` false; an error is a refused input.
pub fn validate_object(init: Option<InitInfo<'_>>, object: &[u8]) -> Result<ObjectReport> {
    let mut report = sys::moq_cmaf_object_report_t::default();
    let init = init.map_or(ptr::null(), |info| {
        info.0 as *const sys::moq_cmaf_init_info_t
    });
    // SAFETY: report is writable; init (when given) and the span are valid
    // for the call.
    let code = unsafe {
        sys::moq_cmaf_object_report_init(&mut report);
        sys::moq_cmaf_validate_object(init, ffi::span(object), &mut report)
    };
    if code != sys::MOQ_OK && code != sys::MOQ_ERR_PROTO {
        return Err(Error::from_code(code));
    }
    Ok(ObjectReport {
        chunk_count: report.chunk_count,
        track_id: report.track_id,
        starts_with_sync: report.starts_with_sync,
        sap_type: SapType::from_raw(report.sap_type),
        valid: report.valid,
        reason: match report.reason {
            sys::MOQ_CMAF_OK => Validity::Ok,
            sys::MOQ_CMAF_ERR_NO_CHUNK => Validity::NoChunk,
            sys::MOQ_CMAF_ERR_MISSING_MFHD => Validity::MissingMfhd,
            sys::MOQ_CMAF_ERR_MULTI_TRACK => Validity::MultiTrack,
            sys::MOQ_CMAF_ERR_TRACK_ID_MISMATCH => Validity::TrackIdMismatch,
            sys::MOQ_CMAF_ERR_MALFORMED => Validity::Malformed,
            sys::MOQ_CMAF_ERR_NO_SAMPLES => Validity::NoSamples,
            code => Validity::Other(code),
        },
    })
}

/// Classify a sample's SAP type from its ISO-BMFF flags alone. A sync sample
/// reports [`SapType::Unknown`]: it is SAP 1 or 2, but flags cannot tell
/// which.
#[must_use]
pub fn sap_from_sample_flags(flags: u32) -> SapType {
    // SAFETY: a pure function.
    SapType::from_raw(unsafe { sys::moq_cmaf_sap_from_sample_flags(flags) })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boxed(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut out = u32::try_from(8 + body.len())
            .expect("a test box fits a 32-bit size")
            .to_be_bytes()
            .to_vec();
        out.extend_from_slice(kind);
        out.extend_from_slice(body);
        out
    }

    /// ftyp + moov(trak(tkhd mdia(mdhd minf(stbl(stsd(<entry>)))))): one
    /// sample entry of type `entry` with a zeroed `fixed`-byte body, an
    /// optional configuration box and, when `original` is given, a
    /// sinf(frma schm schi(tenc)) whose frma names it.
    fn init_segment(
        entry: &[u8; 4],
        fixed: usize,
        config: Option<(&[u8; 4], &[u8])>,
        original: Option<&[u8; 4]>,
    ) -> Vec<u8> {
        let mut body = vec![0u8; fixed];
        if let Some((kind, bytes)) = config {
            body.extend(boxed(kind, bytes));
        }
        if let Some(original) = original {
            let mut tenc = [0u8; 24];
            tenc[6] = 1; // default_isProtected
            for (byte, value) in tenc[8..].iter_mut().zip(0x30u8..) {
                *byte = value; // default_KID
            }
            let sinf = [
                boxed(b"frma", original),
                boxed(b"schm", b"\0\0\0\0cbcs\0\x01\0\0"),
                boxed(b"schi", &boxed(b"tenc", &tenc)),
            ]
            .concat();
            body.extend(boxed(b"sinf", &sinf));
        }
        let stsd = boxed(
            b"stsd",
            &[&[0, 0, 0, 0, 0, 0, 0, 1][..], &boxed(entry, &body)[..]].concat(),
        );
        let mdhd = boxed(
            b"mdhd",
            &[&[0u8; 12][..], &90000u32.to_be_bytes()[..]].concat(),
        );
        let tkhd = boxed(b"tkhd", &[&[0u8; 12][..], &7u32.to_be_bytes()[..]].concat());
        let mdia = boxed(
            b"mdia",
            &[mdhd, boxed(b"minf", &boxed(b"stbl", &stsd))].concat(),
        );
        let trak = boxed(b"trak", &[tkhd, mdia].concat());
        [boxed(b"ftyp", b"isom\0\0\0\0isom"), boxed(b"moov", &trak)].concat()
    }

    #[test]
    fn sample_entries() {
        let avcc: &[u8] = &[0x01, 0x64, 0x00, 0x1f, 0xff];
        let clear = CmafInit::parse(init_segment(b"avc1", 78, Some((b"avcC", avcc)), None))
            .expect("a clear avc1 header parses");
        assert_eq!(clear.info().codec_kind(), CodecKind::Avc);
        assert_eq!(clear.info().sample_entry(), Some(*b"avc1"));
        assert_eq!(clear.info().codec_config(), avcc);
        assert!(clear.info().cenc().is_none());

        let encrypted = CmafInit::parse(init_segment(
            b"encv",
            78,
            Some((b"avcC", avcc)),
            Some(b"avc3"),
        ))
        .expect("an encrypted avc3 header parses");
        assert_eq!(
            encrypted.info().sample_entry(),
            Some(*b"avc3"),
            "the frma original format, not encv"
        );
        assert_eq!(encrypted.info().codec_kind(), CodecKind::Avc);
        assert_eq!(
            encrypted.info().cenc().map(|cenc| cenc.scheme),
            Some(&b"cbcs"[..])
        );

        for entry in [*b"zzzz", [0; 4], [0xff; 4]] {
            let unknown = CmafInit::parse(init_segment(&entry, 16, None, None))
                .expect("a header with an unknown sample entry parses");
            assert_eq!(unknown.info().codec_kind(), CodecKind::Unknown);
            assert_eq!(
                unknown.info().sample_entry(),
                Some(entry),
                "reported as read, zero included"
            );
        }
    }

    #[test]
    fn sample_entry_needs_its_whole_field() {
        let end = ffi::field_end!(sys::moq_cmaf_init_info_t, sample_entry, u32);
        let mut raw = sys::moq_cmaf_init_info_t::default();
        for (stamp, reported) in [
            (sys::MOQ_CMAF_INIT_INFO_V0_SIZE, None),
            (end - 1, None),
            (end, Some([0; 4])),
        ] {
            raw.struct_size = u32::try_from(stamp).expect("a record size fits u32");
            // SAFETY: the record holds no spans.
            let info = unsafe { InitInfo::from_raw(&raw) };
            assert_eq!(info.sample_entry(), reported, "stamp {stamp}");
        }
    }

    #[test]
    fn rejects_garbage() {
        assert_eq!(CmafInit::parse(vec![0u8; 16]).unwrap_err(), Error::PROTO);
        assert!(parse_fragment(&[0u8; 16]).is_err());
        let report = validate_object(None, &[0u8; 16]).expect("a report, not a refusal");
        assert!(!report.valid);
        assert_ne!(report.reason, Validity::Ok);
    }

    #[test]
    fn sap_flags() {
        assert_eq!(sap_from_sample_flags(0x0101_0000), SapType::None);
        assert_eq!(sap_from_sample_flags(0x0200_0000), SapType::Unknown);
        assert!(CmafSample {
            flags: 0x0200_0000,
            ..Default::default()
        }
        .is_sync());
    }
}
