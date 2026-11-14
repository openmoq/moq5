//! MPEG-2 transport stream tracks (draft-gregoire-moq-msfts): the catalog
//! record a track declares, and the record a received catalog carries.

use std::fmt;

use crate::{ffi, sys};

/// mpeg2tsMode (`moq_msf_mpeg2ts_mode_t`). A sender authors the four named
/// modes, and the service tier refuses es-packets on a sender (that
/// authoring is helper-level) and an unrecognized mode.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Mpeg2tsMode {
    /// The catalog named a mode this library does not know; its text is
    /// [`Mpeg2tsRecordRef::mode_name`]. A subscriber must reject the track.
    Unrecognized,
    /// `unmodified-program`.
    UnmodifiedProgram,
    /// `unmodified-multiplex`.
    UnmodifiedMultiplex,
    /// `per-program`.
    PerProgram,
    /// `es-packets`.
    EsPackets,
    /// A value this crate does not know.
    Other(u32),
}

impl Mpeg2tsMode {
    fn raw(self) -> sys::moq_msf_mpeg2ts_mode_t {
        match self {
            Mpeg2tsMode::Unrecognized => sys::MOQ_MSF_MPEG2TS_MODE_UNRECOGNIZED,
            Mpeg2tsMode::UnmodifiedProgram => sys::MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM,
            Mpeg2tsMode::UnmodifiedMultiplex => sys::MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX,
            Mpeg2tsMode::PerProgram => sys::MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM,
            Mpeg2tsMode::EsPackets => sys::MOQ_MSF_MPEG2TS_MODE_ES_PACKETS,
            Mpeg2tsMode::Other(code) => code,
        }
    }

    fn from_raw(code: sys::moq_msf_mpeg2ts_mode_t) -> Mpeg2tsMode {
        match code {
            sys::MOQ_MSF_MPEG2TS_MODE_UNRECOGNIZED => Mpeg2tsMode::Unrecognized,
            sys::MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_PROGRAM => Mpeg2tsMode::UnmodifiedProgram,
            sys::MOQ_MSF_MPEG2TS_MODE_UNMODIFIED_MULTIPLEX => Mpeg2tsMode::UnmodifiedMultiplex,
            sys::MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM => Mpeg2tsMode::PerProgram,
            sys::MOQ_MSF_MPEG2TS_MODE_ES_PACKETS => Mpeg2tsMode::EsPackets,
            code => Mpeg2tsMode::Other(code),
        }
    }

    /// The `moq_msf_mpeg2ts_mode_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        self.raw()
    }
}

/// mpeg2tsTimestampMode (`moq_msf_mpeg2ts_timestamp_mode_t`): what the
/// four-octet prefix of 192-octet source packets holds.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Mpeg2tsTimestampMode {
    /// A name this library does not know (not a rejection: a 192-octet
    /// subscriber treats the prefix as opaque); its text is
    /// [`Mpeg2tsRecordRef::timestamp_mode_name`].
    Unrecognized,
    /// `arrival-time`.
    ArrivalTime,
    /// `opaque`.
    Opaque,
    /// A value this crate does not know.
    Other(u32),
}

impl Mpeg2tsTimestampMode {
    fn raw(self) -> sys::moq_msf_mpeg2ts_timestamp_mode_t {
        match self {
            Mpeg2tsTimestampMode::Unrecognized => sys::MOQ_MSF_MPEG2TS_TIMESTAMP_UNRECOGNIZED,
            Mpeg2tsTimestampMode::ArrivalTime => sys::MOQ_MSF_MPEG2TS_TIMESTAMP_ARRIVAL_TIME,
            Mpeg2tsTimestampMode::Opaque => sys::MOQ_MSF_MPEG2TS_TIMESTAMP_OPAQUE,
            Mpeg2tsTimestampMode::Other(code) => code,
        }
    }

    fn from_raw(code: sys::moq_msf_mpeg2ts_timestamp_mode_t) -> Mpeg2tsTimestampMode {
        match code {
            sys::MOQ_MSF_MPEG2TS_TIMESTAMP_UNRECOGNIZED => Mpeg2tsTimestampMode::Unrecognized,
            sys::MOQ_MSF_MPEG2TS_TIMESTAMP_ARRIVAL_TIME => Mpeg2tsTimestampMode::ArrivalTime,
            sys::MOQ_MSF_MPEG2TS_TIMESTAMP_OPAQUE => Mpeg2tsTimestampMode::Opaque,
            code => Mpeg2tsTimestampMode::Other(code),
        }
    }

    /// The `moq_msf_mpeg2ts_timestamp_mode_t` value.
    #[must_use]
    pub fn code(self) -> u32 {
        self.raw()
    }
}

/// A set of the record's keys (`MOQ_MSF_MPEG2TS_F_*`): the ones a parse
/// found present with a malformed value.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Mpeg2tsKeys(u32);

impl Mpeg2tsKeys {
    /// mpeg2tsMode.
    pub const MODE: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_MODE);
    /// mpeg2tsPacketSize.
    pub const PACKET_SIZE: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_PACKET_SIZE);
    /// mpeg2tsEsPid.
    pub const ES_PID: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_ES_PID);
    /// mpeg2tsProgramNumber.
    pub const PROGRAM_NUMBER: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_PROGRAM_NUMBER);
    /// mpeg2tsPcrPid.
    pub const PCR_PID: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_PCR_PID);
    /// mpeg2tsMuxRate.
    pub const MUX_RATE: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_MUX_RATE);
    /// mpeg2tsSiPids.
    pub const SI_PIDS: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_SI_PIDS);
    /// mpeg2tsRandomAccess.
    pub const RANDOM_ACCESS: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_RANDOM_ACCESS);
    /// mpeg2tsTimestampMode.
    pub const TIMESTAMP_MODE: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_TIMESTAMP_MODE);
    /// mpeg2tsScte35Pid.
    pub const SCTE35_PID: Mpeg2tsKeys = Mpeg2tsKeys(sys::MOQ_MSF_MPEG2TS_F_SCTE35_PID);

    const NAMED: [(Mpeg2tsKeys, &'static str); 10] = [
        (Mpeg2tsKeys::MODE, "MODE"),
        (Mpeg2tsKeys::PACKET_SIZE, "PACKET_SIZE"),
        (Mpeg2tsKeys::ES_PID, "ES_PID"),
        (Mpeg2tsKeys::PROGRAM_NUMBER, "PROGRAM_NUMBER"),
        (Mpeg2tsKeys::PCR_PID, "PCR_PID"),
        (Mpeg2tsKeys::MUX_RATE, "MUX_RATE"),
        (Mpeg2tsKeys::SI_PIDS, "SI_PIDS"),
        (Mpeg2tsKeys::RANDOM_ACCESS, "RANDOM_ACCESS"),
        (Mpeg2tsKeys::TIMESTAMP_MODE, "TIMESTAMP_MODE"),
        (Mpeg2tsKeys::SCTE35_PID, "SCTE35_PID"),
    ];

    /// A set from its `MOQ_MSF_MPEG2TS_F_*` bits; unknown bits are kept.
    #[must_use]
    pub const fn from_bits(bits: u32) -> Mpeg2tsKeys {
        Mpeg2tsKeys(bits)
    }
    /// The `MOQ_MSF_MPEG2TS_F_*` bits.
    #[must_use]
    pub const fn bits(self) -> u32 {
        self.0
    }
    /// No key.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
    /// Every key of `keys` is in the set.
    #[must_use]
    pub const fn contains(self, keys: Mpeg2tsKeys) -> bool {
        self.0 & keys.0 == keys.0
    }
}

// The key names, and the bits of keys this crate does not know.
impl fmt::Debug for Mpeg2tsKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut set = f.debug_set();
        let mut rest = self.0;
        for (key, name) in Mpeg2tsKeys::NAMED {
            if self.contains(key) {
                set.entry(&format_args!("{name}"));
                rest &= !key.0;
            }
        }
        if rest != 0 {
            set.entry(&format_args!("{rest:#x}"));
        }
        set.finish()
    }
}

/// The MPEG-TS record a sender's track declares in its catalog entry
/// (`moq_msf_mpeg2ts_t`, carried as the track's `MOQ_MSF_EXT_MPEG2TS`
/// extension). The service tier checks it as a subscriber would
/// (`moq_msf_track_mpeg2ts_check`) and refuses a record that fails with
/// `MOQ_ERR_INVAL` when the track is added; in byte-stream mode it declares
/// mpeg2tsRandomAccess itself, and a value here that contradicts it is
/// refused too.
///
/// It is the application's record, so it is not `#[non_exhaustive]`: start
/// from [`new`](Mpeg2tsRecord::new) and set the optional keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mpeg2tsRecord {
    /// mpeg2tsMode (required).
    pub mode: Mpeg2tsMode,
    /// mpeg2tsPacketSize, 188 or 192 (required).
    pub packet_size: u32,
    /// mpeg2tsEsPid: es-packets only, which a sender does not author.
    pub es_pid: Option<u32>,
    /// mpeg2tsProgramNumber, 1..=65535; a per-program track needs it.
    pub program_number: Option<u32>,
    /// mpeg2tsPcrPid.
    pub pcr_pid: Option<u32>,
    /// mpeg2tsMuxRate, bit/s; absent in unmodified-multiplex.
    pub mux_rate: Option<u64>,
    /// mpeg2tsSiPids, each listed once.
    pub si_pids: Option<Vec<u32>>,
    /// mpeg2tsRandomAccess.
    pub random_access: Option<bool>,
    /// mpeg2tsTimestampMode; absent at 188 octets.
    pub timestamp_mode: Option<Mpeg2tsTimestampMode>,
    /// mpeg2tsScte35Pid.
    pub scte35_pid: Option<u32>,
}

impl Mpeg2tsRecord {
    /// A record with its two required keys and no optional one.
    #[must_use]
    pub fn new(mode: Mpeg2tsMode, packet_size: u32) -> Mpeg2tsRecord {
        Mpeg2tsRecord {
            mode,
            packet_size,
            es_pid: None,
            program_number: None,
            pcr_pid: None,
            mux_rate: None,
            si_pids: None,
            random_access: None,
            timestamp_mode: None,
            scte35_pid: None,
        }
    }

    /// The C record, sized and stamped by the library's initializer. Its
    /// SI PID array is a copy of [`si_pids`](Mpeg2tsRecord::si_pids) held in
    /// `si_pids` (cleared first), so the record is valid while that vector
    /// is alive and unchanged. Producing it is safe; passing it to libmoq is
    /// the caller's `unsafe` call.
    #[must_use]
    pub fn as_raw(&self, si_pids: &mut Vec<u32>) -> sys::moq_msf_mpeg2ts_t {
        si_pids.clear();
        si_pids.extend(self.si_pids.iter().flatten());
        let mut raw = sys::moq_msf_mpeg2ts_t::default();
        // SAFETY: a writable struct of exactly the size passed.
        unsafe { sys::moq_msf_mpeg2ts_init_sized(&mut raw, std::mem::size_of_val(&raw)) };
        raw.has_mode = true;
        raw.mode = self.mode.raw();
        raw.has_packet_size = true;
        raw.packet_size = self.packet_size;
        if let Some(pid) = self.es_pid {
            raw.has_es_pid = true;
            raw.es_pid = pid;
        }
        if let Some(number) = self.program_number {
            raw.has_program_number = true;
            raw.program_number = number;
        }
        if let Some(pid) = self.pcr_pid {
            raw.has_pcr_pid = true;
            raw.pcr_pid = pid;
        }
        if let Some(rate) = self.mux_rate {
            raw.has_mux_rate = true;
            raw.mux_rate = rate;
        }
        if self.si_pids.is_some() {
            raw.has_si_pids = true;
            if !si_pids.is_empty() {
                raw.si_pids = si_pids.as_mut_ptr();
                raw.si_pid_count = si_pids.len();
            }
        }
        if let Some(random_access) = self.random_access {
            raw.has_random_access = true;
            raw.random_access = random_access;
        }
        if let Some(mode) = self.timestamp_mode {
            raw.has_timestamp_mode = true;
            raw.timestamp_mode = mode.raw();
        }
        if let Some(pid) = self.scte35_pid {
            raw.has_scte35_pid = true;
            raw.scte35_pid = pid;
        }
        raw
    }
}

/// A received track's MPEG-TS record, borrowed from its receiver: every key
/// the catalog carried, as parsed, without a verdict (a track the record
/// fails has no delivery path, see its description). Keys a value of the
/// wrong type or range made malformed are listed by
/// [`malformed`](Mpeg2tsRecordRef::malformed), present with a zero value.
#[derive(Clone, Copy)]
pub struct Mpeg2tsRecordRef<'receiver>(&'receiver sys::moq_msf_mpeg2ts_t);

impl<'receiver> Mpeg2tsRecordRef<'receiver> {
    /// A view of `raw` when its stamp covers the first published record.
    ///
    /// # Safety
    /// The record's mode and timestamp-mode names and its SI PID array must
    /// be valid for reads for `'receiver` wherever their presence flags are
    /// set.
    #[must_use]
    pub unsafe fn from_raw(raw: &'receiver sys::moq_msf_mpeg2ts_t) -> Option<Self> {
        ffi::covers(raw.struct_size, sys::MOQ_MSF_MPEG2TS_V0_SIZE).then_some(Mpeg2tsRecordRef(raw))
    }

    /// mpeg2tsMode; `None` when absent.
    #[must_use]
    pub fn mode(&self) -> Option<Mpeg2tsMode> {
        self.0.has_mode.then(|| Mpeg2tsMode::from_raw(self.0.mode))
    }
    /// The mode's text as the catalog spelled it; `None` when absent.
    #[must_use]
    pub fn mode_name(&self) -> Option<&'receiver [u8]> {
        // SAFETY: valid for 'receiver by from_raw's contract.
        self.0
            .has_mode
            .then(|| unsafe { ffi::bytes(self.0.mode_name) })
    }
    /// mpeg2tsPacketSize.
    #[must_use]
    pub fn packet_size(&self) -> Option<u32> {
        self.0.has_packet_size.then_some(self.0.packet_size)
    }
    /// mpeg2tsEsPid.
    #[must_use]
    pub fn es_pid(&self) -> Option<u32> {
        self.0.has_es_pid.then_some(self.0.es_pid)
    }
    /// mpeg2tsProgramNumber.
    #[must_use]
    pub fn program_number(&self) -> Option<u32> {
        self.0.has_program_number.then_some(self.0.program_number)
    }
    /// mpeg2tsPcrPid.
    #[must_use]
    pub fn pcr_pid(&self) -> Option<u32> {
        self.0.has_pcr_pid.then_some(self.0.pcr_pid)
    }
    /// mpeg2tsMuxRate, bit/s.
    #[must_use]
    pub fn mux_rate(&self) -> Option<u64> {
        self.0.has_mux_rate.then_some(self.0.mux_rate)
    }
    /// mpeg2tsSiPids (a malformed element reads 0).
    #[must_use]
    pub fn si_pids(&self) -> Option<&'receiver [u32]> {
        // SAFETY: as for mode_name.
        self.0
            .has_si_pids
            .then(|| unsafe { ffi::array(self.0.si_pids, self.0.si_pid_count) })
    }
    /// mpeg2tsRandomAccess.
    #[must_use]
    pub fn random_access(&self) -> Option<bool> {
        self.0.has_random_access.then_some(self.0.random_access)
    }
    /// mpeg2tsTimestampMode.
    #[must_use]
    pub fn timestamp_mode(&self) -> Option<Mpeg2tsTimestampMode> {
        self.0
            .has_timestamp_mode
            .then(|| Mpeg2tsTimestampMode::from_raw(self.0.timestamp_mode))
    }
    /// The timestamp mode's text as the catalog spelled it.
    #[must_use]
    pub fn timestamp_mode_name(&self) -> Option<&'receiver [u8]> {
        // SAFETY: as for mode_name.
        self.0
            .has_timestamp_mode
            .then(|| unsafe { ffi::bytes(self.0.timestamp_mode_name) })
    }
    /// mpeg2tsScte35Pid.
    #[must_use]
    pub fn scte35_pid(&self) -> Option<u32> {
        self.0.has_scte35_pid.then_some(self.0.scte35_pid)
    }
    /// The keys present with a malformed value.
    #[must_use]
    pub fn malformed(&self) -> Mpeg2tsKeys {
        Mpeg2tsKeys(self.0.malformed)
    }
    /// The raw record; its spans borrow what `from_raw` was given.
    #[must_use]
    pub fn raw(&self) -> &'receiver sys::moq_msf_mpeg2ts_t {
        self.0
    }
}

// The keys, not the raw spans a derive would print.
impl fmt::Debug for Mpeg2tsRecordRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Mpeg2tsRecordRef")
            .field("mode", &self.mode())
            .field("packet_size", &self.packet_size())
            .field("program_number", &self.program_number())
            .field("random_access", &self.random_access())
            .field("malformed", &self.malformed())
            .finish_non_exhaustive()
    }
}

/// One packaging-extension record of a received track
/// (`moq_msf_track_ext_t`), borrowed from its owner.
#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
pub enum TrackExtension<'receiver> {
    /// The MPEG-TS record.
    Mpeg2ts(Mpeg2tsRecordRef<'receiver>),
    /// A kind this crate does not read, or an MPEG-TS entry whose record is
    /// missing or below its first published size.
    Other(u32),
}

impl<'receiver> TrackExtension<'receiver> {
    /// Read one extension record: an MPEG-TS entry whose record is present
    /// and covers the first published size is
    /// [`TrackExtension::Mpeg2ts`], anything else its kind.
    ///
    /// # Safety
    /// For an MPEG-TS entry, `raw.u.mpeg2ts` must be null or point to a
    /// record valid for `'receiver` that meets
    /// [`Mpeg2tsRecordRef::from_raw`]'s contract.
    #[must_use]
    pub unsafe fn from_raw(raw: &'receiver sys::moq_msf_track_ext_t) -> TrackExtension<'receiver> {
        if raw.kind == sys::MOQ_MSF_EXT_MPEG2TS {
            // SAFETY: the kind selects the union member; the record is valid
            // for 'receiver by this function's contract.
            if let Some(record) = unsafe { raw.u.mpeg2ts.as_ref() } {
                // SAFETY: the record meets Mpeg2tsRecordRef::from_raw's
                // contract by this function's.
                if let Some(record) = unsafe { Mpeg2tsRecordRef::from_raw(record) } {
                    return TrackExtension::Mpeg2ts(record);
                }
            }
        }
        TrackExtension::Other(raw.kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modes_round_trip() {
        let named = [
            Mpeg2tsMode::Unrecognized,
            Mpeg2tsMode::UnmodifiedProgram,
            Mpeg2tsMode::UnmodifiedMultiplex,
            Mpeg2tsMode::PerProgram,
            Mpeg2tsMode::EsPackets,
        ];
        for mode in named {
            assert_eq!(Mpeg2tsMode::from_raw(mode.code()), mode);
        }
        assert_eq!(Mpeg2tsMode::from_raw(9), Mpeg2tsMode::Other(9));
        for mode in [
            Mpeg2tsTimestampMode::Unrecognized,
            Mpeg2tsTimestampMode::ArrivalTime,
            Mpeg2tsTimestampMode::Opaque,
        ] {
            assert_eq!(Mpeg2tsTimestampMode::from_raw(mode.code()), mode);
        }
        assert_eq!(
            Mpeg2tsTimestampMode::from_raw(7),
            Mpeg2tsTimestampMode::Other(7)
        );
    }

    #[test]
    fn keys_are_a_set() {
        let keys =
            Mpeg2tsKeys::from_bits(Mpeg2tsKeys::MODE.bits() | Mpeg2tsKeys::SI_PIDS.bits() | 0x8000);
        assert!(keys.contains(Mpeg2tsKeys::MODE));
        assert!(!keys.contains(Mpeg2tsKeys::PCR_PID));
        assert!(Mpeg2tsKeys::default().is_empty());
        assert_eq!(format!("{keys:?}"), "{MODE, SI_PIDS, 0x8000}");
    }

    #[test]
    fn a_record_states_its_keys() {
        let mut record = Mpeg2tsRecord::new(Mpeg2tsMode::PerProgram, 192);
        record.program_number = Some(7);
        record.si_pids = Some(vec![0x20, 0x21]);
        record.timestamp_mode = Some(Mpeg2tsTimestampMode::ArrivalTime);
        let mut pids = vec![9, 9, 9];
        let raw = record.as_raw(&mut pids);
        assert_eq!(pids, [0x20, 0x21], "the record's PIDs, copied");
        assert!(raw.struct_size as usize >= sys::MOQ_MSF_MPEG2TS_V0_SIZE);
        assert!(raw.has_mode && raw.mode == sys::MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM);
        assert!(raw.has_packet_size && raw.packet_size == 192);
        assert!(raw.has_program_number && raw.program_number == 7);
        assert!(raw.has_si_pids && raw.si_pid_count == 2);
        assert!(raw.has_timestamp_mode);
        assert!(!raw.has_es_pid && !raw.has_pcr_pid && !raw.has_mux_rate);
        assert!(!raw.has_random_access && !raw.has_scte35_pid);
        // SAFETY: the record's array borrows `pids`, alive here.
        let view = unsafe { Mpeg2tsRecordRef::from_raw(&raw) }.expect("a full record");
        assert_eq!(view.mode(), Some(Mpeg2tsMode::PerProgram));
        assert_eq!(view.si_pids(), Some(&[0x20, 0x21][..]));
        assert_eq!(view.es_pid(), None);
        let mut short = raw;
        short.struct_size = 8;
        // SAFETY: as above.
        assert!(unsafe { Mpeg2tsRecordRef::from_raw(&short) }.is_none());
    }
}
