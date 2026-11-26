//! The LOCMAF catalog record (draft-einarsson-moq-locmaf-01 section 5): the
//! locmafVersion a received catalog carries for a track.

use std::fmt;

use crate::{ffi, sys};

/// A received track's LOCMAF record (`moq_msf_locmaf_t`, carried as the
/// track's `MOQ_MSF_EXT_LOCMAF` extension), borrowed from its owner: the
/// locmafVersion as the catalog carried it, without a verdict. A track whose
/// version the library does not support has no delivery path (see its
/// description); a sender's track declares the version itself.
#[derive(Clone, Copy)]
pub struct LocmafRecordRef<'receiver>(&'receiver sys::moq_msf_locmaf_t);

impl<'receiver> LocmafRecordRef<'receiver> {
    /// A view of `raw` when its stamp covers the first published record.
    ///
    /// # Safety
    /// The record's version span must be valid for reads for `'receiver`
    /// when `has_version` is set.
    #[must_use]
    pub unsafe fn from_raw(raw: &'receiver sys::moq_msf_locmaf_t) -> Option<Self> {
        ffi::covers(raw.struct_size, sys::MOQ_MSF_LOCMAF_V0_SIZE).then_some(LocmafRecordRef(raw))
    }

    /// locmafVersion as the catalog spelled it; `None` when absent, and
    /// empty when the value was not a JSON string
    /// ([`version_malformed`](LocmafRecordRef::version_malformed)).
    #[must_use]
    pub fn version(&self) -> Option<&'receiver [u8]> {
        // SAFETY: valid for 'receiver by from_raw's contract.
        self.0
            .has_version
            .then(|| unsafe { ffi::bytes(self.0.version) })
    }

    /// locmafVersion is present with a value that is not a JSON string.
    #[must_use]
    pub fn version_malformed(&self) -> bool {
        self.0.malformed & sys::MOQ_MSF_LOCMAF_F_VERSION != 0
    }

    /// The raw record; its span borrows what `from_raw` was given, and its
    /// `malformed` bits are kept whole.
    #[must_use]
    pub fn raw(&self) -> &'receiver sys::moq_msf_locmaf_t {
        self.0
    }
}

// The version as text, not the raw span a derive would print.
impl fmt::Debug for LocmafRecordRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocmafRecordRef")
            .field("version", &self.version().map(String::from_utf8_lossy))
            .field("version_malformed", &self.version_malformed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_reads_within_its_stamp() {
        let version = b"0.3";
        let record = sys::moq_msf_locmaf_t {
            struct_size: std::mem::size_of::<sys::moq_msf_locmaf_t>() as u32,
            has_version: true,
            version: ffi::span(version),
            malformed: 0,
        };
        // SAFETY: the record's span borrows `version`, alive here.
        let view = unsafe { LocmafRecordRef::from_raw(&record) }.expect("a whole record");
        assert_eq!(view.version(), Some(&version[..]));
        assert!(!view.version_malformed());
        assert_eq!(
            format!("{view:?}"),
            "LocmafRecordRef { version: Some(\"0.3\"), version_malformed: false }"
        );

        let malformed = sys::moq_msf_locmaf_t {
            version: sys::moq_bytes_t::default(),
            malformed: sys::MOQ_MSF_LOCMAF_F_VERSION | 0x80,
            ..record
        };
        // SAFETY: the record holds no span.
        let view = unsafe { LocmafRecordRef::from_raw(&malformed) }.expect("a whole record");
        assert_eq!(view.version(), Some(&b""[..]), "present, empty");
        assert!(view.version_malformed());
        assert_eq!(view.raw().malformed & 0x80, 0x80, "unknown bits are kept");

        let absent = sys::moq_msf_locmaf_t {
            has_version: false,
            ..record
        };
        // SAFETY: as above.
        let view = unsafe { LocmafRecordRef::from_raw(&absent) }.expect("a whole record");
        assert_eq!(view.version(), None);

        let short = sys::moq_msf_locmaf_t {
            struct_size: (sys::MOQ_MSF_LOCMAF_V0_SIZE - 1) as u32,
            ..record
        };
        // SAFETY: as above.
        assert!(unsafe { LocmafRecordRef::from_raw(&short) }.is_none());
    }
}
