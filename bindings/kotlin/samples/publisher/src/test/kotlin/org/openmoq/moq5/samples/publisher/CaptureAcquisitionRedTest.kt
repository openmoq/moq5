package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The acquisition boundary: platform constants and a measured calibration,
 * exercised entirely on the host.
 *
 * Nothing here starts a camera, a microphone or an encoder, and nothing reads a
 * real clock: the elapsed-realtime and wall-clock readings are scripted, which
 * is what makes the MEASUREMENT PROTOCOL — not a timing coincidence — the thing
 * under test.
 *
 * Constant authority, from the local SDK sources:
 * `AudioTimestamp.TIMEBASE_MONOTONIC` 0, `TIMEBASE_BOOTTIME` 1;
 * `CameraMetadata.SENSOR_INFO_TIMESTAMP_SOURCE_UNKNOWN` 0, `..._REALTIME` 1.
 */
class CaptureAcquisitionRedTest {

    private val epoch = UnixMicros(1_746_104_600_000_000uL)

    /** Elapsed readings in the order the protocol takes them. */
    private class ScriptedElapsed(vararg readings: Long) : ElapsedRealtimeSource {
        private val values = readings.toList()
        var calls = 0
            private set

        override fun nanos(): Long {
            val at = if (calls < values.size) values[calls] else values.last()
            calls++
            return at
        }
    }

    private class ScriptedWall(private val value: UnixMicros, private val onRead: () -> Unit = {}) : WallClockSource {
        var calls = 0
            private set

        override fun micros(): UnixMicros {
            calls++
            onRead()
            return value
        }
    }

    // ---- b: the platform constants -------------------------------------------

    @Test
    fun b1_theAudioTimebaseConstantsMapToTheModelsValues() {
        assertEquals(
            TimebaseOutcome.Resolved(PlatformTimebase.ElapsedRealtime),
            AndroidCaptureFacts.audioTimebase(1),
            "TIMEBASE_BOOTTIME is SystemClock.elapsedRealtimeNanos",
        )
        assertEquals(
            TimebaseOutcome.Resolved(PlatformTimebase.Monotonic),
            AndroidCaptureFacts.audioTimebase(0),
            "TIMEBASE_MONOTONIC is System.nanoTime",
        )
        val refused = assertIs<TimebaseOutcome.Unsupported>(
            AndroidCaptureFacts.audioTimebase(7),
            "an unrecognised timebase constant is refused, not inferred",
        )
        assertTrue(refused.reason.contains("7"), refused.reason)
    }

    @Test
    fun b2_theCameraTimestampSourceMapsAndUnknownStaysRefusedForEpochPlacement() {
        assertEquals(
            TimebaseOutcome.Resolved(PlatformTimebase.ElapsedRealtime),
            AndroidCaptureFacts.cameraTimebase(AndroidCaptureConstants.SENSOR_TIMESTAMP_SOURCE_REALTIME),
        )
        assertEquals(
            TimebaseOutcome.Resolved(PlatformTimebase.Unknown),
            AndroidCaptureFacts.cameraTimebase(AndroidCaptureConstants.SENSOR_TIMESTAMP_SOURCE_UNKNOWN),
            "the fact is carried; the refusal belongs to the accepted clock",
        )
        // ... and that refusal is the accepted model's, not a second copy of it.
        assertIs<CaptureCapability.Unavailable>(CaptureCapabilities.cameraTimestamps(PlatformTimebase.Unknown))
        val clock =
            PlatformCaptureClock(ClockCalibration(1_000_000_000L, epoch, 120L))
        assertIs<CaptureInstant.Unavailable>(clock.instantOf(PlatformTimebase.Unknown, 1_100_000_000L))

        assertIs<TimebaseOutcome.Unsupported>(
            AndroidCaptureFacts.cameraTimebase(9),
            "an unrecognised timestamp-source constant is refused, not inferred",
        )
    }

    @Test
    fun b3_theApiLevelDecisionIsTheAcceptedOne() {
        val unavailable = assertIs<CaptureCapability.Unavailable>(AndroidCaptureFacts.audioCapability(23))
        assertTrue(unavailable.reason.contains("24"), unavailable.reason)
        assertEquals(CaptureCapability.Available, AndroidCaptureFacts.audioCapability(24))
        assertEquals(
            CaptureCapabilities.audioTimestamps(23),
            AndroidCaptureFacts.audioCapability(23),
            "one decision, not a second copy of it",
        )
    }

    // ---- m: the measured calibration ----------------------------------------

    @Test
    fun m1_theWallClockIsBracketedAndTheSpanIsTheUncertainty() {
        val elapsed = ScriptedElapsed(1_000_000_000L, 1_000_240_000L)
        val wall = ScriptedWall(epoch)
        val measured = assertIs<CalibrationOutcome.Measured>(Calibrator.measure(elapsed, wall))
        assertEquals(240L, measured.calibration.uncertaintyMicros, "the measured span, not a chosen constant")
        assertEquals(
            1_000_120_000L,
            measured.calibration.elapsedRealtimeNanos,
            "the calibration point is the middle of the bracket",
        )
        assertEquals(epoch, measured.calibration.epoch)
        assertEquals(2, elapsed.calls, "exactly two elapsed readings")
        assertEquals(1, wall.calls, "and exactly one wall reading")
    }

    @Test
    fun m2_theWallClockIsReadBetweenTheTwoElapsedReadings() {
        // The ordering IS the contract: a wall reading taken after the media
        // timestamp, or outside the bracket, is not a calibration.
        val order = ArrayList<String>()
        val elapsed =
            object : ElapsedRealtimeSource {
                private val values = listOf(1_000_000_000L, 1_000_200_000L)
                private var calls = 0

                override fun nanos(): Long {
                    order.add("elapsed")
                    return values[if (calls < values.size) calls++ else values.size - 1]
                }
            }
        val wall = ScriptedWall(epoch) { order.add("wall") }
        assertIs<CalibrationOutcome.Measured>(Calibrator.measure(elapsed, wall))
        assertEquals(listOf("elapsed", "wall", "elapsed"), order)
    }

    @Test
    fun m3_aSpanBeyondThePolicyBoundIsUnusable() {
        val wide = 1_000_000_000L + (CaptureClockPolicy.MAX_UNCERTAINTY_US + 1) * 1_000L
        val outcome = Calibrator.measure(ScriptedElapsed(1_000_000_000L, wide), ScriptedWall(epoch))
        val unusable = assertIs<CalibrationOutcome.Unusable>(outcome, "a bracket this wide is not a calibration")
        assertTrue(unusable.reason.isNotEmpty())
    }

    @Test
    fun m4_aBackwardsBracketIsUnusableEvenBelowOneMicrosecond() {
        assertIs<CalibrationOutcome.Unusable>(
            Calibrator.measure(ScriptedElapsed(1_000_000_000L, 999_000_000L), ScriptedWall(epoch)),
            "the elapsed-realtime clock does not go backwards",
        )
        // A backwards bracket smaller than a microsecond truncates to a span of
        // ZERO, so the span check alone would let it through with a calibration
        // point BEFORE its own first reading.
        assertIs<CalibrationOutcome.Unusable>(
            Calibrator.measure(ScriptedElapsed(1_000_000_000L, 999_999_500L), ScriptedWall(epoch)),
            "a sub-microsecond backwards bracket is still backwards",
        )
    }

    @Test
    fun m6_aSubMicrosecondBracketDoesNotClaimZeroUncertainty() {
        // 999 ns of span is not an exact calibration. Reporting 0 us would
        // claim a precision the measurement does not have.
        val measured = assertIs<CalibrationOutcome.Measured>(
            Calibrator.measure(ScriptedElapsed(1_000_000_000L, 1_000_000_999L), ScriptedWall(epoch)),
        )
        assertEquals(1L, measured.calibration.uncertaintyMicros, "any nonzero span is at least one microsecond")
    }

    @Test
    fun m7_aBracketOneNanosecondPastTheBoundIsUnusable() {
        val after = 1_000_000_000L + CaptureClockPolicy.MAX_UNCERTAINTY_US * 1_000L + 1L
        assertIs<CalibrationOutcome.Unusable>(
            Calibrator.measure(ScriptedElapsed(1_000_000_000L, after), ScriptedWall(epoch)),
            "one nanosecond past the bound is past the bound",
        )
        // And the bound itself is still usable, so the row is a boundary, not a shift.
        val atBound = 1_000_000_000L + CaptureClockPolicy.MAX_UNCERTAINTY_US * 1_000L
        val measured = assertIs<CalibrationOutcome.Measured>(
            Calibrator.measure(ScriptedElapsed(1_000_000_000L, atBound), ScriptedWall(epoch)),
        )
        assertEquals(CaptureClockPolicy.MAX_UNCERTAINTY_US, measured.calibration.uncertaintyMicros)
    }

    @Test
    fun m5_theMeasuredCalibrationDrivesTheAcceptedClockWithoutReadingAnythingElse() {
        val elapsed = ScriptedElapsed(1_000_000_000L, 1_000_100_000L)
        val wall = ScriptedWall(epoch)
        val measured = assertIs<CalibrationOutcome.Measured>(Calibrator.measure(elapsed, wall))
        val clock = PlatformCaptureClock(measured.calibration)

        // 33,333 us after the calibration point, on the platform's own stamp.
        val at = assertIs<CaptureInstant.At>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_033_383_000L))
        assertEquals(UnixMicros(epoch.value + 33_333uL), at.epoch)
        assertEquals(100L, at.uncertaintyMicros, "the measured span travels with the instant")
        assertEquals(2, elapsed.calls, "placing a frame reads no clock at all")
        assertEquals(1, wall.calls)
    }
}
