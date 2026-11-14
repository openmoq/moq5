//! The session's clock: microseconds the host supplies.

use std::fmt;
use std::time::Duration;

/// A point on the host's own microsecond clock.
///
/// The session reads no clock: every advancing call takes the current time
/// from the host as an `Instant`, and the session's deadlines come back in
/// the same unit. The origin is the host's choice (a monotonic clock, a
/// simulator's counter); only differences and order mean anything. This is
/// not [`std::time::Instant`], which reads the system clock.
///
/// `u64::MAX` is the value the session uses for "no deadline", so
/// [`Session::next_deadline`](crate::Session::next_deadline) reports it as
/// `None`; it is still a valid time to pass in.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(u64);

impl Instant {
    /// The clock's origin.
    pub const ZERO: Instant = Instant(0);

    /// The time `micros` microseconds after the origin.
    #[must_use]
    pub const fn from_micros(micros: u64) -> Instant {
        Instant(micros)
    }

    /// Microseconds since the origin.
    #[must_use]
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    /// The time `duration` later, or `None` when the result is past
    /// `u64::MAX` microseconds or `duration` is not a whole number of
    /// microseconds: nothing is rounded.
    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<Instant> {
        let nanos = duration.as_nanos();
        if nanos % 1_000 != 0 {
            return None;
        }
        let micros = u64::try_from(nanos / 1_000).ok()?;
        self.0.checked_add(micros).map(Instant)
    }

    /// The time `micros` microseconds later, or `None` past `u64::MAX`.
    #[must_use]
    pub const fn checked_add_micros(self, micros: u64) -> Option<Instant> {
        match self.0.checked_add(micros) {
            Some(value) => Some(Instant(value)),
            None => None,
        }
    }

    /// How long after `earlier` this time is; zero when it is not later.
    #[must_use]
    pub fn saturating_duration_since(self, earlier: Instant) -> Duration {
        Duration::from_micros(self.0.saturating_sub(earlier.0))
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instant({}us)", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arithmetic_is_exact() {
        let t = Instant::from_micros(10);
        assert_eq!(t.as_micros(), 10);
        assert_eq!(
            t.checked_add(Duration::from_micros(5)),
            Some(Instant::from_micros(15))
        );
        assert_eq!(t.checked_add(Duration::from_nanos(1_500)), None);
        assert_eq!(Instant::from_micros(u64::MAX).checked_add_micros(1), None);
        assert_eq!(
            Instant::from_micros(3).saturating_duration_since(t),
            Duration::ZERO
        );
        assert_eq!(
            t.saturating_duration_since(Instant::ZERO),
            Duration::from_micros(10)
        );
        assert_eq!(format!("{t:?}"), "Instant(10us)");
    }
}
