//! MPEG-2 transport stream tracks on the service tier: how a sender takes
//! its media, and what it counts. The catalog record a track declares is
//! the media crate's ([`Mpeg2tsRecord`](crate::Mpeg2tsRecord)).

use std::fmt;
use std::time::Duration;

use crate::{ffi, Error, Result};

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
