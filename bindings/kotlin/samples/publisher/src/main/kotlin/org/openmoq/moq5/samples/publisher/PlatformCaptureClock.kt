package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.UnixMicros

/**
 * How this sample turns a PLATFORM timestamp into the epoch capture instant the
 * RAW/LOC policy requires (LOC-01 2.3.1.1, and the ruling carried from 1762).
 *
 * Android hands out media timestamps on clocks that are NOT the wall clock:
 *
 *  - a camera frame is stamped on the source named by
 *    `SENSOR_INFO_TIMESTAMP_SOURCE`. `REALTIME` is the
 *    `SystemClock.elapsedRealtimeNanos` base. `UNKNOWN` is monotonic and
 *    roughly in the `SystemClock.uptimeMillis` timebase, not accurately
 *    comparable to other subsystems or devices -- enough for A/V
 *    synchronisation, by Android's own statement, but not a relationship to
 *    the epoch;
 *  - an `AudioTimestamp` is on `TIMEBASE_BOOTTIME` (the same
 *    `elapsedRealtimeNanos` base) or `TIMEBASE_MONOTONIC` (the same units and
 *    timebase as `System.nanoTime`, which does not advance across suspend).
 *
 * So an epoch instant exists only through an explicit CALIBRATION: one reading
 * of the wall clock beside one reading of the elapsed-realtime clock, with the
 * span between those two reads carried as the uncertainty. Nothing here reads a
 * clock itself -- the calibration is supplied by the caller that took it -- and
 * nothing substitutes a callback's arrival time for a capture time.
 *
 * Two rules exist because they are easy to get wrong and impossible to see
 * afterwards:
 *
 *  - **already-placed instants are never rewritten.** A later calibration
 *    applies to frames converted after it; a timestamp from before the
 *    recalibration moment is refused rather than silently re-placed.
 *  - **a jump is named.** The wall clock may step (NTP, a user edit), so a
 *    recalibration beyond the declared bound is a discontinuity the caller has
 *    to decide about, not an invisible retime.
 *
 * These are host-testable value rules. That the platform really reports what is
 * modelled here is a device fact and stays an instrumented gate.
 */
internal object CaptureClockPolicy {
    /** The sample modules' Android floor. Raising it is a separate decision. */
    const val SAMPLE_MIN_SDK: Int = 23

    /** `AudioRecord.getTimestamp(AudioTimestamp, int)` exists from API 24. */
    const val AUDIO_TIMESTAMP_SDK: Int = 24

    /** A calibration wider than this is refused rather than carried. */
    const val MAX_UNCERTAINTY_US: Long = 2_000

    /** A recalibration step beyond this is a named discontinuity. */
    const val MAX_STEP_US: Long = 50_000

    /** How far an audio timestamp may be extrapolated to another frame. */
    const val MAX_AUDIO_EXTRAPOLATION_US: Long = 500_000
}

/** Which platform clock a timestamp is on. */
internal enum class PlatformTimebase {
    /** `SystemClock.elapsedRealtimeNanos`: camera `REALTIME`, audio `TIMEBASE_BOOTTIME`. */
    ElapsedRealtime,

    /**
     * `AudioTimestamp.TIMEBASE_MONOTONIC`: the same units and timebase as
     * `System.nanoTime`, which does not advance across suspend.
     */
    Monotonic,

    /**
     * The camera reported `SENSOR_INFO_TIMESTAMP_SOURCE_UNKNOWN`: monotonic
     * nanoseconds roughly in the `SystemClock.uptimeMillis` timebase, not
     * accurately comparable to other subsystems or devices. Android says that
     * is accurate enough for A/V synchronisation against the audio subsystem;
     * it is NOT a relationship to the epoch, which is what a LOC capture
     * timestamp needs, so this sample refuses it for that use.
     */
    Unknown,
}

/**
 * One calibration of the elapsed-realtime clock against the wall clock, taken
 * by the caller at a stated moment. [uncertaintyMicros] is the span of the two
 * readings, measured -- never a constant chosen to look precise.
 */
internal data class ClockCalibration(
    val elapsedRealtimeNanos: Long,
    val epoch: UnixMicros,
    val uncertaintyMicros: Long,
)

/** The capture instant of one frame, or why this sample will not name one. */
internal sealed interface CaptureInstant {
    data class At(val epoch: UnixMicros, val uncertaintyMicros: Long) : CaptureInstant

    data class Unavailable(val reason: String) : CaptureInstant
}

/** What a later calibration did. */
internal sealed interface Recalibration {
    /** Accepted, and applied only to frames converted after it. */
    data class Applied(val stepMicros: Long) : Recalibration

    /** Accepted as a JUMP: the caller decides; nothing already placed moves. */
    data class Discontinuity(val stepMicros: Long) : Recalibration

    data class Refused(val reason: String) : Recalibration
}

/** Whether the platform can give this sample what it needs. */
internal sealed interface CaptureCapability {
    data object Available : CaptureCapability

    data class Unavailable(val reason: String) : CaptureCapability
}

/** One `AudioTimestamp`: a frame position and the instant that frame reached the endpoint. */
internal data class AudioTimestampFacts(
    val framePosition: Long,
    val timestampNanos: Long,
    val timebase: PlatformTimebase,
)

/** Adds two signed values, or `null` when the sum does not fit. */
private fun addOrNull(a: Long, b: Long): Long? {
    val sum = a + b
    if (((a xor sum) and (b xor sum)) < 0) return null
    return sum
}

/** Subtracts, or `null` when the difference does not fit. */
private fun subOrNull(a: Long, b: Long): Long? {
    val diff = a - b
    if (((a xor b) and (a xor diff)) < 0) return null
    return diff
}

/** Shifts an epoch instant by a signed microsecond offset, refusing to wrap. */
private fun shiftOrNull(epoch: UnixMicros, offsetMicros: Long): UnixMicros? {
    if (offsetMicros >= 0) {
        val room = ULong.MAX_VALUE - epoch.value
        if (offsetMicros.toULong() > room) return null
        return UnixMicros(epoch.value + offsetMicros.toULong())
    }
    val back = (-offsetMicros).toULong()
    if (back > epoch.value) return null
    return UnixMicros(epoch.value - back)
}

/**
 * The publisher's clock: a calibration, the conversions it licenses, and the
 * recalibration rules.
 */
internal class PlatformCaptureClock(calibration: ClockCalibration) {
    private var current: ClockCalibration = calibration

    /** The calibration in force. */
    val calibration: ClockCalibration
        get() = current

    /** The elapsed-realtime moment of the latest accepted recalibration. */
    var recalibratedAtNanos: Long? = null
        private set

    fun instantOf(timebase: PlatformTimebase, timestampNanos: Long): CaptureInstant {
        when (timebase) {
            PlatformTimebase.ElapsedRealtime -> Unit
            PlatformTimebase.Monotonic ->
                return CaptureInstant.Unavailable(
                    "this stamp is on the System.nanoTime monotonic base, which does not advance across " +
                        "suspend and has no fixed relationship to the calibrated elapsed-realtime base",
                )
            PlatformTimebase.Unknown ->
                return CaptureInstant.Unavailable(
                    "the camera reports an UNKNOWN timestamp source: comparable enough for A/V sync, but " +
                        "not accurately comparable across subsystems and not a relationship to the epoch",
                )
        }
        val usable = Calibrations.check(current)
        if (usable is CaptureCapability.Unavailable) {
            // The bound belongs to the clock, not to every caller remembering
            // to check first: an instant carrying an invalid uncertainty would
            // claim precision this calibration does not have.
            return CaptureInstant.Unavailable(usable.reason)
        }
        val boundary = recalibratedAtNanos
        if (boundary != null && timestampNanos < boundary) {
            // Re-placing it on the new calibration would retime media that was
            // already published on the old one.
            return CaptureInstant.Unavailable(
                "this stamp precedes the latest recalibration; it belongs to the relationship it was placed on",
            )
        }
        val deltaNanos = subOrNull(timestampNanos, current.elapsedRealtimeNanos)
            ?: return CaptureInstant.Unavailable("the elapsed-realtime difference does not fit")
        val epoch = shiftOrNull(current.epoch, deltaNanos / 1_000L)
            ?: return CaptureInstant.Unavailable("the resulting epoch instant does not fit")
        return CaptureInstant.At(epoch, current.uncertaintyMicros)
    }

    fun recalibrate(next: ClockCalibration): Recalibration {
        val usable = Calibrations.check(next)
        if (usable is CaptureCapability.Unavailable) return Recalibration.Refused(usable.reason)
        if (next.elapsedRealtimeNanos <= current.elapsedRealtimeNanos) {
            return Recalibration.Refused("the elapsed-realtime clock does not go backwards")
        }
        val deltaNanos = subOrNull(next.elapsedRealtimeNanos, current.elapsedRealtimeNanos)
            ?: return Recalibration.Refused("the elapsed-realtime difference does not fit")
        val expected = shiftOrNull(current.epoch, deltaNanos / 1_000L)
            ?: return Recalibration.Refused("the previous relationship cannot reach this moment")
        val step =
            if (next.epoch.value >= expected.value) {
                val forward = next.epoch.value - expected.value
                if (forward > Long.MAX_VALUE.toULong()) return Recalibration.Refused("the step does not fit")
                forward.toLong()
            } else {
                val back = expected.value - next.epoch.value
                if (back > Long.MAX_VALUE.toULong()) return Recalibration.Refused("the step does not fit")
                -back.toLong()
            }
        // POLICY: a jump is ACCEPTED as the newer truth about the wall clock and
        // becomes the calibration in force for later frames; it is NAMED so the
        // caller can decide what it means for media already in flight. Nothing
        // already placed moves either way, and a stamp from before this moment
        // is refused rather than re-placed (see instantOf).
        current = next
        recalibratedAtNanos = next.elapsedRealtimeNanos
        val magnitude = if (step < 0) -step else step
        return if (magnitude > CaptureClockPolicy.MAX_STEP_US) {
            Recalibration.Discontinuity(step)
        } else {
            Recalibration.Applied(step)
        }
    }
}

/** Whether a calibration may be used at all. */
internal object Calibrations {
    fun check(calibration: ClockCalibration): CaptureCapability =
        when {
            calibration.uncertaintyMicros < 0 ->
                CaptureCapability.Unavailable("a negative reading span is not a measurement")
            calibration.uncertaintyMicros > CaptureClockPolicy.MAX_UNCERTAINTY_US ->
                CaptureCapability.Unavailable(
                    "this calibration spans " + calibration.uncertaintyMicros + " us, beyond the " +
                        CaptureClockPolicy.MAX_UNCERTAINTY_US + " us this sample will carry",
                )
            else -> CaptureCapability.Available
        }
}

/** The capture capabilities of one Android API level. */
internal object CaptureCapabilities {
    /** `AudioRecord.getTimestamp` is API 24+; on 23 this sample says so and refuses. */
    fun audioTimestamps(sdkInt: Int): CaptureCapability =
        if (sdkInt >= CaptureClockPolicy.AUDIO_TIMESTAMP_SDK) {
            CaptureCapability.Available
        } else {
            CaptureCapability.Unavailable(
                "AudioRecord.getTimestamp needs API 24; this device reports API " + sdkInt +
                    ", so audio has no capture instant here",
            )
        }

    /** A camera whose timestamp source is not REALTIME cannot be related to the epoch. */
    fun cameraTimestamps(timebase: PlatformTimebase): CaptureCapability =
        if (timebase == PlatformTimebase.ElapsedRealtime) {
            CaptureCapability.Available
        } else {
            CaptureCapability.Unavailable(
                "only SENSOR_INFO_TIMESTAMP_SOURCE_REALTIME shares the calibrated elapsed-realtime base; " +
                    "this camera reports " + timebase,
            )
        }
}

/** One audio frame's instant on the elapsed-realtime base, or why it has none. */
internal sealed interface FrameElapsed {
    data class At(val nanos: Long) : FrameElapsed

    data class Unavailable(val reason: String) : FrameElapsed
}

/** One audio frame's capture instant, from the endpoint timestamp it belongs to. */
internal object AudioFrameClock {
    /**
     * [frameIndex]'s instant on the elapsed-realtime base the ledger files
     * under: the same checked arithmetic and frame-distance bound as
     * [instantOf], without the epoch placement.
     */
    fun elapsedNanosOf(timestamp: AudioTimestampFacts, frameIndex: Long, sampleRate: Int): FrameElapsed {
        if (timestamp.timebase != PlatformTimebase.ElapsedRealtime) {
            return FrameElapsed.Unavailable("the audio timestamp is on " + timestamp.timebase + ", not the elapsed-realtime base")
        }
        if (sampleRate <= 0) {
            return FrameElapsed.Unavailable("a sample rate of " + sampleRate + " places no frame")
        }
        val frames = subOrNull(frameIndex, timestamp.framePosition)
            ?: return FrameElapsed.Unavailable("the frame distance does not fit")
        if (frames > Long.MAX_VALUE / 1_000_000L || frames < Long.MIN_VALUE / 1_000_000L) {
            return FrameElapsed.Unavailable("the frame distance does not convert")
        }
        val offsetMicros = frames * 1_000_000L / sampleRate
        val magnitude = if (offsetMicros < 0) -offsetMicros else offsetMicros
        if (magnitude > CaptureClockPolicy.MAX_AUDIO_EXTRAPOLATION_US) {
            return FrameElapsed.Unavailable(
                "this frame is " + magnitude + " us from the reported one, beyond the " +
                    CaptureClockPolicy.MAX_AUDIO_EXTRAPOLATION_US + " us this sample will extrapolate",
            )
        }
        if (offsetMicros > Long.MAX_VALUE / 1_000L || offsetMicros < Long.MIN_VALUE / 1_000L) {
            return FrameElapsed.Unavailable("the frame offset does not convert to nanoseconds")
        }
        val frameNanos = addOrNull(timestamp.timestampNanos, offsetMicros * 1_000L)
            ?: return FrameElapsed.Unavailable("the frame's elapsed-realtime instant does not fit")
        return FrameElapsed.At(frameNanos)
    }

    fun instantOf(
        clock: PlatformCaptureClock,
        timestamp: AudioTimestampFacts,
        frameIndex: Long,
        sampleRate: Int,
    ): CaptureInstant {
        if (timestamp.timebase != PlatformTimebase.ElapsedRealtime) {
            return clock.instantOf(timestamp.timebase, timestamp.timestampNanos)
        }
        return when (val elapsed = elapsedNanosOf(timestamp, frameIndex, sampleRate)) {
            is FrameElapsed.At -> clock.instantOf(PlatformTimebase.ElapsedRealtime, elapsed.nanos)
            is FrameElapsed.Unavailable -> CaptureInstant.Unavailable(elapsed.reason)
        }
    }
}
