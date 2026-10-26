package org.openmoq.moq5.samples.publisher

import android.media.AudioTimestamp
import android.os.SystemClock
import org.openmoq.moq5.UnixMicros

/**
 * The boundary between the Android platform's own facts and the accepted clock
 * model in `PlatformCaptureClock.kt`.
 *
 * It is deliberately thin and free of platform state: every platform value is
 * PASSED IN — the timestamp-source constant the camera reported, the timebase
 * the `AudioTimestamp` names, the running `Build.VERSION.SDK_INT` — so the
 * decisions can be exercised on the host without a device, an encoder, or a
 * permission flow. The Activity that eventually reads those values does nothing
 * but hand them here.
 *
 * The audio timebase constants come from `android.media.AudioTimestamp` itself,
 * and the camera timestamp-source constants now come from `CameraMetadata`
 * through [CameraTimestampSources] — one source of truth, so a renumbering
 * cannot drift from this sample.
 */
internal object AndroidCaptureConstants {
    /** `CameraMetadata.SENSOR_INFO_TIMESTAMP_SOURCE_UNKNOWN`, from the platform. */
    const val SENSOR_TIMESTAMP_SOURCE_UNKNOWN: Int = CameraTimestampSources.UNKNOWN

    /** `CameraMetadata.SENSOR_INFO_TIMESTAMP_SOURCE_REALTIME`, from the platform. */
    const val SENSOR_TIMESTAMP_SOURCE_REALTIME: Int = CameraTimestampSources.REALTIME
}

/** A platform timebase constant resolved into the model's own value, or refused. */
internal sealed interface TimebaseOutcome {
    data class Resolved(val timebase: PlatformTimebase) : TimebaseOutcome

    data class Unsupported(val reason: String) : TimebaseOutcome
}

/** A measured calibration, or why the measurement cannot be used. */
internal sealed interface CalibrationOutcome {
    data class Measured(val calibration: ClockCalibration) : CalibrationOutcome

    data class Unusable(val reason: String) : CalibrationOutcome
}

/** One reading of the elapsed-realtime clock, in nanoseconds. */
internal fun interface ElapsedRealtimeSource {
    fun nanos(): Long
}

/** One reading of the wall clock, in epoch microseconds. */
internal fun interface WallClockSource {
    fun micros(): UnixMicros
}

/** The platform's own readers. Never called by a host test. */
internal object AndroidClockSources {
    val elapsedRealtime: ElapsedRealtimeSource = ElapsedRealtimeSource { SystemClock.elapsedRealtimeNanos() }
}

/** Platform constants translated into the accepted model's values. */
internal object AndroidCaptureFacts {
    /** `AudioTimestamp.TIMEBASE_BOOTTIME` / `TIMEBASE_MONOTONIC`; anything else is refused. */
    fun audioTimebase(constant: Int): TimebaseOutcome =
        when (constant) {
            AudioTimestamp.TIMEBASE_BOOTTIME -> TimebaseOutcome.Resolved(PlatformTimebase.ElapsedRealtime)
            AudioTimestamp.TIMEBASE_MONOTONIC -> TimebaseOutcome.Resolved(PlatformTimebase.Monotonic)
            else ->
                TimebaseOutcome.Unsupported(
                    "AudioTimestamp reports timebase " + constant + ", which this sample does not know; " +
                        "guessing one would place media on a clock nobody named",
                )
        }

    /**
     * The camera's `SENSOR_INFO_TIMESTAMP_SOURCE`. `REALTIME` is the calibrated
     * base; `UNKNOWN` resolves to the model's own [PlatformTimebase.Unknown],
     * which the accepted clock refuses for epoch placement. An unrecognised
     * constant is refused, never inferred.
     */
    fun cameraTimebase(constant: Int): TimebaseOutcome =
        when (constant) {
            AndroidCaptureConstants.SENSOR_TIMESTAMP_SOURCE_REALTIME ->
                TimebaseOutcome.Resolved(PlatformTimebase.ElapsedRealtime)
            AndroidCaptureConstants.SENSOR_TIMESTAMP_SOURCE_UNKNOWN ->
                TimebaseOutcome.Resolved(PlatformTimebase.Unknown)
            else ->
                TimebaseOutcome.Unsupported(
                    "the camera reports timestamp source " + constant + ", which this sample does not know",
                )
        }

    /** The running API level's audio-timestamp capability: the accepted decision, not a copy. */
    fun audioCapability(sdkInt: Int): CaptureCapability = CaptureCapabilities.audioTimestamps(sdkInt)
}

/**
 * Measures one calibration by BRACKETING the wall-clock reading between two
 * elapsed-realtime readings.
 *
 * The bracket is the whole point: the span between the two elapsed readings is
 * the measured uncertainty — never a chosen constant — and the calibration's
 * elapsed point is the middle of that bracket. Because the wall clock is read
 * ONLY here, and only between those two readings, no callback's arrival time
 * can become a capture instant.
 */
internal object Calibrator {
    fun measure(elapsed: ElapsedRealtimeSource, wall: WallClockSource): CalibrationOutcome {
        val before = elapsed.nanos()
        val epoch = wall.micros()
        val after = elapsed.nanos()
        if (after < before) {
            return CalibrationOutcome.Unusable("the elapsed-realtime clock read backwards across the bracket")
        }
        val spanNanos = after - before
        // CONSERVATIVE: any nonzero span rounds UP to the next whole
        // microsecond. Flooring would report a 999 ns bracket as an exact
        // calibration, and would let a bracket one nanosecond past the policy
        // bound floor back onto it.
        val spanMicros = (spanNanos + 999L) / 1_000L
        val calibration =
            ClockCalibration(
                elapsedRealtimeNanos = before + spanNanos / 2L,
                epoch = epoch,
                uncertaintyMicros = spanMicros,
            )
        val usable = Calibrations.check(calibration)
        if (usable is CaptureCapability.Unavailable) return CalibrationOutcome.Unusable(usable.reason)
        return CalibrationOutcome.Measured(calibration)
    }
}
