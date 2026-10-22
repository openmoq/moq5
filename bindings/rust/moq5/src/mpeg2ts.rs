//! MPEG-2 transport stream tracks (draft-gregoire-moq-msfts): the catalog
//! record a track declares, how a sender takes its media, and what the
//! receive side reports about one.

use std::fmt;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::{ffi, sys};

/// mpeg2tsMode (`moq_msf_mpeg2ts_mode_t`). A sender authors the four named
/// modes, and the service refuses es-packets on a sender (that authoring is
/// helper-level) and an unrecognized mode.
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
/// extension). The service checks it as a subscriber would
/// (`moq_msf_track_mpeg2ts_check`) and refuses a record that fails with
/// [`Error::INVAL`] at `add_track`; in byte-stream mode it declares
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

    /// The C record, sized and stamped by the library's initializer, with
    /// its SI PID array borrowing `si_pids` (valid while that is).
    pub(crate) fn raw(&self, si_pids: &mut [u32]) -> sys::moq_msf_mpeg2ts_t {
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
    pub(crate) fn from_raw(raw: &'receiver sys::moq_msf_mpeg2ts_t) -> Option<Self> {
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
        // SAFETY: the record and its spans are owned by the track handle,
        // valid for the receiver's life.
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
        // SAFETY: the array is owned by the track handle, as above.
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
    /// The raw record; its spans borrow the track handle.
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
/// (`moq_msf_track_ext_t`), borrowed from its receiver.
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
    pub(crate) fn from_raw(raw: &'receiver sys::moq_msf_track_ext_t) -> TrackExtension<'receiver> {
        if raw.kind == sys::MOQ_MSF_EXT_MPEG2TS {
            // SAFETY: the kind selects the union member; the record is owned
            // by the track handle, valid for the receiver's life.
            let record = unsafe { raw.u.mpeg2ts.as_ref() };
            if let Some(record) = record.and_then(Mpeg2tsRecordRef::from_raw) {
                return TrackExtension::Mpeg2ts(record);
            }
        }
        TrackExtension::Other(raw.kind)
    }
}

/// How a sender's MPEG-TS track takes its media; each track takes one of
/// the two calls, and the other returns [`Error::WRONG_STATE`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mpeg2tsInput {
    /// The application hands TS bytes to
    /// [`Sender::write_mpeg2ts`](crate::Sender::write_mpeg2ts) in any
    /// chunking, and the service forms Objects and Groups (opening a Group
    /// at each random access point, with the PAT/PMT run before it).
    ByteStream {
        /// Source packets per Object at most; 0 selects 64.
        object_packets: u32,
        /// The shortest Group in PCR time, whole milliseconds that fit a
        /// `u32` (anything else is [`Error::INVAL`]); zero opens a Group at
        /// every random access point.
        min_group: Duration,
        /// Forward the packets before the first random access point, which
        /// otherwise are discarded so every Group starts at one (which is
        /// also what makes the service declare mpeg2tsRandomAccess).
        forward_before_random_access: bool,
    },
    /// The application forms each Object and writes it with
    /// [`Sender::write`](crate::Sender::write), which checks it: whole
    /// source packets with their sync bytes, and with mpeg2tsRandomAccess
    /// declared a random access point at each Group start.
    AppObjects,
}

impl Default for Mpeg2tsInput {
    /// The byte stream with the service's defaults.
    fn default() -> Mpeg2tsInput {
        Mpeg2tsInput::ByteStream {
            object_packets: 0,
            min_group: Duration::ZERO,
            forward_before_random_access: false,
        }
    }
}

impl Mpeg2tsInput {
    /// The four track configuration fields this selects, in order:
    /// app objects, object packets, minimum group milliseconds, forward
    /// before random access.
    pub(crate) fn raw(self) -> Result<(bool, u32, u32, bool)> {
        match self {
            Mpeg2tsInput::AppObjects => Ok((true, 0, 0, false)),
            Mpeg2tsInput::ByteStream {
                object_packets,
                min_group,
                forward_before_random_access,
            } => {
                let min_group_ms =
                    u32::try_from(ffi::millis(min_group)?).map_err(|_| Error::INVAL)?;
                Ok((
                    false,
                    object_packets,
                    min_group_ms,
                    forward_before_random_access,
                ))
            }
        }
    }
}

/// A failed [`Sender::write_mpeg2ts`](crate::Sender::write_mpeg2ts): the
/// error, and the bytes of the input the service took anyway. Every result
/// reports them, as C writes them on every return: after a lost Object
/// ([`Error::NOMEM`]) they cover the packet that completed it; at a missing
/// sync byte ([`Error::PROTO`]) they stop before that packet; an interrupt,
/// or a companion refusal latched while the call yielded between Objects
/// ([`Error::UNSUPPORTED`]), reports what was taken before it. Resume from
/// that offset; the bytes before it must not be written again. Converting
/// the failure into an [`Error`] (as `?` does) drops the count, so read it
/// first. The pair is the whole record; it is not `#[non_exhaustive]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
pub struct Mpeg2tsWriteError {
    /// What libmoq reported.
    pub error: Error,
    /// The bytes of the input the service now owns.
    pub consumed: usize,
}

impl fmt::Display for Mpeg2tsWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MPEG-TS write failed after {} bytes: {}",
            self.consumed, self.error
        )
    }
}

impl std::error::Error for Mpeg2tsWriteError {}

impl From<Mpeg2tsWriteError> for Error {
    fn from(failure: Mpeg2tsWriteError) -> Error {
        failure.error
    }
}

/// The MPEG-TS counters of a sender's track ([`TrackStats::mpeg2ts`]);
/// zero for another packaging. The packetizer's (groups through
/// `source_pcr_discontinuities`) count in byte-stream mode only.
///
/// [`TrackStats::mpeg2ts`]: crate::TrackStats::mpeg2ts
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Mpeg2tsTrackStats {
    /// Groups the packetizer opened.
    pub groups: u64,
    /// Objects the packetizer emitted.
    pub objects: u64,
    /// Source packets it was given.
    pub packets_in: u64,
    /// Packets discarded before the first random access point.
    pub packets_dropped_before_random_access: u64,
    /// The longest Group, in PCR time (27 MHz).
    pub max_group_27mhz: u64,
    /// Groups longer than two seconds.
    pub groups_over_2s: u64,
    /// The peak rate between PCRs, bit/s.
    pub peak_rate_bps: u64,
    /// PCR discontinuities in the source.
    pub source_pcr_discontinuities: u64,
    /// The record declares an mpeg2tsMuxRate below the peak rate. MSF does
    /// not let a published track's fields change, so this is only reported.
    pub mux_rate_below_peak: bool,
    /// The carried program left the PAT (per-program). MSFTS says the
    /// publisher should then end the track, which is left to the
    /// application.
    pub program_gone: bool,
    /// Objects the track holds now after a short write (0 to 2); a writer
    /// with no more input passes empty data until this is 0.
    pub objects_parked: u64,
    /// Bytes of incomplete packets discarded.
    pub partial_bytes_discarded: u64,
    /// Later Objects of a Group that lost queued media, discarded until the
    /// next Group.
    pub continuation_objects_discarded: u64,
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
        let mut pids = record.si_pids.clone().unwrap_or_default();
        let raw = record.raw(&mut pids);
        assert!(raw.struct_size as usize >= sys::MOQ_MSF_MPEG2TS_V0_SIZE);
        assert!(raw.has_mode && raw.mode == sys::MOQ_MSF_MPEG2TS_MODE_PER_PROGRAM);
        assert!(raw.has_packet_size && raw.packet_size == 192);
        assert!(raw.has_program_number && raw.program_number == 7);
        assert!(raw.has_si_pids && raw.si_pid_count == 2);
        assert!(raw.has_timestamp_mode);
        assert!(!raw.has_es_pid && !raw.has_pcr_pid && !raw.has_mux_rate);
        assert!(!raw.has_random_access && !raw.has_scte35_pid);
        let view = Mpeg2tsRecordRef::from_raw(&raw).expect("a full record");
        assert_eq!(view.mode(), Some(Mpeg2tsMode::PerProgram));
        assert_eq!(view.si_pids(), Some(&[0x20, 0x21][..]));
        assert_eq!(view.es_pid(), None);
        let mut short = raw;
        short.struct_size = 8;
        assert!(Mpeg2tsRecordRef::from_raw(&short).is_none());
    }

    #[test]
    fn inputs_select_the_track_fields() {
        assert_eq!(Mpeg2tsInput::AppObjects.raw(), Ok((true, 0, 0, false)));
        assert_eq!(Mpeg2tsInput::default().raw(), Ok((false, 0, 0, false)));
        let input = Mpeg2tsInput::ByteStream {
            object_packets: 7,
            min_group: Duration::from_millis(500),
            forward_before_random_access: true,
        };
        assert_eq!(input.raw(), Ok((false, 7, 500, true)));
        let fractional = Mpeg2tsInput::ByteStream {
            object_packets: 0,
            min_group: Duration::from_micros(1500),
            forward_before_random_access: false,
        };
        assert_eq!(fractional.raw(), Err(Error::INVAL));
        let too_long = Mpeg2tsInput::ByteStream {
            object_packets: 0,
            min_group: Duration::from_millis(u64::from(u32::MAX) + 1),
            forward_before_random_access: false,
        };
        assert_eq!(too_long.raw(), Err(Error::INVAL));
    }
}
