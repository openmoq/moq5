package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The platform capture-clock contract, over ordinary values.
 *
 * Every platform fact is supplied: the calibration the caller took, the
 * timebase the platform named, and the timestamp the frame carried. Nothing in
 * these rows reads a clock, and nothing claims that a device really reports
 * what is modelled here — that is the instrumented gate, still owed.
 *
 * The reference calibration below says: at elapsed-realtime 1,000,000,000 ns
 * (one second since boot) the wall clock read 1,746,104,600,000,000 us, and the
 * two readings were 120 us apart.
 */
class PlatformCaptureClockRedTest {

    private val epoch = UnixMicros(1_746_104_600_000_000uL)
    private val calibration =
        ClockCalibration(elapsedRealtimeNanos = 1_000_000_000L, epoch = epoch, uncertaintyMicros = 120L)

    private fun clock() = PlatformCaptureClock(calibration)

    // ---- p: the conversion ---------------------------------------------------

    @Test
    fun p1_anElapsedRealtimeStampBecomesAnEpochInstantOnTheCalibration() {
        // 33,333 us after the calibration point.
        val instant = clock().instantOf(PlatformTimebase.ElapsedRealtime, 1_033_333_000L)
        val at = assertIs<CaptureInstant.At>(instant)
        assertEquals(UnixMicros(epoch.value + 33_333uL), at.epoch, "the elapsed delta, in microseconds")
        assertEquals(120L, at.uncertaintyMicros, "the calibration's measured span is carried, not reduced")
    }

    @Test
    fun p2_anEarlierStampThanTheCalibrationIsStillOnTheSameRelationship() {
        val instant = clock().instantOf(PlatformTimebase.ElapsedRealtime, 999_000_000L)
        val at = assertIs<CaptureInstant.At>(instant)
        assertEquals(UnixMicros(epoch.value - 1_000uL), at.epoch, "one millisecond before the calibration point")
    }

    @Test
    fun p3_aMonotonicStampIsNotRelatableAndIsRefusedByName() {
        // TIMEBASE_MONOTONIC stops during deep sleep, so it has no fixed
        // relationship to the elapsed-realtime base this calibration used.
        val unavailable = assertIs<CaptureInstant.Unavailable>(
            clock().instantOf(PlatformTimebase.Monotonic, 1_033_333_000L),
            "a monotonic stamp must not be read as an elapsed-realtime one",
        )
        assertTrue(unavailable.reason.contains("monotonic", ignoreCase = true), unavailable.reason)
    }

    @Test
    fun p4_anUnknownCameraTimestampSourceIsARefusalNotAnInference() {
        assertIs<CaptureInstant.Unavailable>(
            clock().instantOf(PlatformTimebase.Unknown, 1_033_333_000L),
            "SENSOR_INFO_TIMESTAMP_SOURCE_UNKNOWN cannot be related to the epoch",
        )
        assertIs<CaptureCapability.Unavailable>(CaptureCapabilities.cameraTimestamps(PlatformTimebase.Unknown))
        assertEquals(CaptureCapability.Available, CaptureCapabilities.cameraTimestamps(PlatformTimebase.ElapsedRealtime))
    }

    @Test
    fun p5_aCalibrationWiderThanThePolicyBoundIsRefused() {
        val loose = calibration.copy(uncertaintyMicros = CaptureClockPolicy.MAX_UNCERTAINTY_US + 1)
        val refused = assertIs<CaptureCapability.Unavailable>(
            Calibrations.check(loose),
            "a calibration this wide would claim precision it does not have",
        )
        assertTrue(refused.reason.isNotEmpty())
        assertEquals(CaptureCapability.Available, Calibrations.check(calibration))
        assertIs<CaptureCapability.Unavailable>(
            Calibrations.check(calibration.copy(uncertaintyMicros = -1)),
            "a negative span is not a measurement",
        )
    }

    // ---- r: recalibration ----------------------------------------------------

    @Test
    fun r1_aLaterCalibrationAppliesOnlyToFramesConvertedAfterIt() {
        val clock = clock()
        val before = assertIs<CaptureInstant.At>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_100_000_000L))
        assertEquals(UnixMicros(epoch.value + 100_000uL), before.epoch)

        // The wall clock has drifted 5 ms ahead of this relationship.
        val next =
            ClockCalibration(
                elapsedRealtimeNanos = 2_000_000_000L,
                epoch = UnixMicros(epoch.value + 1_005_000uL),
                uncertaintyMicros = 80L,
            )
        val applied = assertIs<Recalibration.Applied>(clock.recalibrate(next))
        assertEquals(5_000L, applied.stepMicros, "the step is stated, not hidden")

        val after = assertIs<CaptureInstant.At>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 2_100_000_000L))
        assertEquals(UnixMicros(epoch.value + 1_105_000uL), after.epoch, "later frames use the later calibration")
        assertEquals(80L, after.uncertaintyMicros)
        assertEquals(UnixMicros(epoch.value + 100_000uL), before.epoch, "and the instant already placed is unchanged")
    }

    @Test
    fun r2_aStampFromBeforeTheRecalibrationIsRefusedRatherThanRePlaced() {
        val clock = clock()
        assertIs<CaptureInstant.At>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_100_000_000L))
        clock.recalibrate(
            ClockCalibration(2_000_000_000L, UnixMicros(epoch.value + 1_005_000uL), 80L),
        )
        val refused = assertIs<CaptureInstant.Unavailable>(
            clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_500_000_000L),
            "that instant belongs to the previous relationship; re-placing it would retime accepted media",
        )
        assertTrue(refused.reason.isNotEmpty())
        assertEquals(2_000_000_000L, clock.recalibratedAtNanos)
    }

    @Test
    fun r3_aJumpBeyondTheBoundIsNamedNotSilentlyApplied() {
        val clock = clock()
        val jump =
            ClockCalibration(
                elapsedRealtimeNanos = 2_000_000_000L,
                epoch = UnixMicros(epoch.value + 1_000_000uL + CaptureClockPolicy.MAX_STEP_US.toULong() + 1uL),
                uncertaintyMicros = 80L,
            )
        val discontinuity = assertIs<Recalibration.Discontinuity>(
            clock.recalibrate(jump),
            "Android allows the wall clock to step; the caller decides what that means",
        )
        assertEquals(CaptureClockPolicy.MAX_STEP_US + 1, discontinuity.stepMicros)
    }

    @Test
    fun r4_aRecalibrationThatGoesBackwardsOrIsTooWideIsRefused() {
        val clock = clock()
        assertIs<Recalibration.Refused>(
            clock.recalibrate(ClockCalibration(500_000_000L, epoch, 80L)),
            "the elapsed-realtime clock does not go backwards",
        )
        assertIs<Recalibration.Refused>(
            clock.recalibrate(
                ClockCalibration(2_000_000_000L, epoch, CaptureClockPolicy.MAX_UNCERTAINTY_US + 1),
            ),
        )
        assertEquals(calibration, clock.calibration, "a refused calibration does not take effect")
        assertNull(clock.recalibratedAtNanos)
    }

    @Test
    fun r5_anInvalidInitialCalibrationCanNeverProduceAnInstant() {
        // The bound is the clock's own, not something every caller has to
        // remember to check before constructing one.
        for (span in listOf(CaptureClockPolicy.MAX_UNCERTAINTY_US + 1, -1L)) {
            val clock = PlatformCaptureClock(calibration.copy(uncertaintyMicros = span))
            val refused = assertIs<CaptureInstant.Unavailable>(
                clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_033_333_000L),
                "an instant carrying an invalid uncertainty is not an instant",
            )
            assertTrue(refused.reason.isNotEmpty())
            assertIs<CaptureInstant.Unavailable>(
                AudioFrameClock.instantOf(
                    clock,
                    AudioTimestampFacts(48_000L, 2_000_000_000L, PlatformTimebase.ElapsedRealtime),
                    frameIndex = 48_000L,
                    sampleRate = 48_000,
                ),
                "and the audio path cannot route around it",
            )
        }
    }

    @Test
    fun r6_anInvalidStartRecoversOnlyThroughAValidRecalibration() {
        val clock = PlatformCaptureClock(calibration.copy(uncertaintyMicros = -1))
        assertIs<CaptureInstant.Unavailable>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_100_000_000L))
        val good = ClockCalibration(2_000_000_000L, UnixMicros(epoch.value + 1_000_000uL), 90L)
        assertIs<Recalibration.Applied>(clock.recalibrate(good), "a valid calibration is accepted from any start")
        val at = assertIs<CaptureInstant.At>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 2_050_000_000L))
        assertEquals(UnixMicros(epoch.value + 1_050_000uL), at.epoch)
        assertEquals(90L, at.uncertaintyMicros)
    }

    @Test
    fun r7_aDiscontinuityBecomesTheActiveCalibrationForLaterFrames() {
        // The declared policy: a jump is ACCEPTED as the newer truth about the
        // wall clock and NAMED so the caller can decide what it means for media
        // in flight. Nothing already placed moves, and a stamp from before the
        // jump is refused rather than re-placed.
        val clock = clock()
        val before = assertIs<CaptureInstant.At>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_100_000_000L))
        assertEquals(UnixMicros(epoch.value + 100_000uL), before.epoch)

        val jumped =
            ClockCalibration(
                elapsedRealtimeNanos = 2_000_000_000L,
                epoch = UnixMicros(epoch.value + 1_000_000uL + 60_000uL),
                uncertaintyMicros = 70L,
            )
        assertEquals(Recalibration.Discontinuity(60_000L), clock.recalibrate(jumped))
        assertEquals(jumped, clock.calibration, "the jump is the calibration in force")

        val after = assertIs<CaptureInstant.At>(clock.instantOf(PlatformTimebase.ElapsedRealtime, 2_100_000_000L))
        assertEquals(
            UnixMicros(epoch.value + 1_160_000uL),
            after.epoch,
            "a later frame is placed on the jumped relationship, not the old one",
        )
        assertEquals(70L, after.uncertaintyMicros)
        assertIs<CaptureInstant.Unavailable>(
            clock.instantOf(PlatformTimebase.ElapsedRealtime, 1_900_000_000L),
            "and a stamp from before the jump is refused, never re-placed",
        )
        assertEquals(UnixMicros(epoch.value + 100_000uL), before.epoch, "the instant already placed is unchanged")
    }

    // ---- a: the audio association -------------------------------------------

    @Test
    fun a1_anAudioFrameIsPlacedFromItsTimestampsFramePosition() {
        // The endpoint reported frame 48,000 at elapsed-realtime 2 s. At
        // 48 kHz, frame 50,400 is 50 ms later.
        val timestamp = AudioTimestampFacts(48_000L, 2_000_000_000L, PlatformTimebase.ElapsedRealtime)
        val at = assertIs<CaptureInstant.At>(
            AudioFrameClock.instantOf(clock(), timestamp, frameIndex = 50_400L, sampleRate = 48_000),
        )
        assertEquals(UnixMicros(epoch.value + 1_050_000uL), at.epoch, "1 s after the calibration, plus 50 ms")
    }

    @Test
    fun a2_anEarlierFrameThanTheTimestampIsPlacedBeforeIt() {
        val timestamp = AudioTimestampFacts(48_000L, 2_000_000_000L, PlatformTimebase.ElapsedRealtime)
        val at = assertIs<CaptureInstant.At>(
            AudioFrameClock.instantOf(clock(), timestamp, frameIndex = 24_000L, sampleRate = 48_000),
        )
        assertEquals(UnixMicros(epoch.value + 500_000uL), at.epoch, "half a second earlier")
    }

    @Test
    fun a3_anAudioTimestampOnTheMonotonicTimebaseIsRefused() {
        val timestamp = AudioTimestampFacts(48_000L, 2_000_000_000L, PlatformTimebase.Monotonic)
        assertIs<CaptureInstant.Unavailable>(
            AudioFrameClock.instantOf(clock(), timestamp, frameIndex = 48_000L, sampleRate = 48_000),
            "TIMEBASE_MONOTONIC is not the calibrated base",
        )
    }

    @Test
    fun a4_anImpossibleSampleRateOrAnOverlongExtrapolationIsRefused() {
        val timestamp = AudioTimestampFacts(48_000L, 2_000_000_000L, PlatformTimebase.ElapsedRealtime)
        assertIs<CaptureInstant.Unavailable>(
            AudioFrameClock.instantOf(clock(), timestamp, frameIndex = 48_000L, sampleRate = 0),
            "a zero sample rate places nothing",
        )
        val farAway = 48_000L + 48L * (CaptureClockPolicy.MAX_AUDIO_EXTRAPOLATION_US / 1_000L) + 48L
        assertIs<CaptureInstant.Unavailable>(
            AudioFrameClock.instantOf(clock(), timestamp, frameIndex = farAway, sampleRate = 48_000),
            "extrapolating this far from the reported frame is a guess",
        )
    }

    // ---- c: capability by API level -----------------------------------------

    @Test
    fun c1_apiTwentyThreeHasNoAudioTimestampAndSaysSo() {
        val unavailable = assertIs<CaptureCapability.Unavailable>(
            CaptureCapabilities.audioTimestamps(CaptureClockPolicy.SAMPLE_MIN_SDK),
            "AudioRecord.getTimestamp arrived in API 24; on 23 there is no audio capture instant",
        )
        assertTrue(unavailable.reason.contains("24"), unavailable.reason)
        assertEquals(CaptureCapability.Available, CaptureCapabilities.audioTimestamps(CaptureClockPolicy.AUDIO_TIMESTAMP_SDK))
        assertEquals(23, CaptureClockPolicy.SAMPLE_MIN_SDK, "the sample floor is not raised to reach this API")
    }

    @Test
    fun c2_theRefusalIsWhatReachesThePublishDecision() {
        // A frame with no capture instant is refused by the publish contract,
        // which is where the two halves meet: no instant, no media object.
        val buffer = ByteArray(8) { 0x11 }
        val decision =
            EncodedOutputs.decide(
                EncodedOutput(buffer, 0, buffer.size, 1_000L, CaptureContract.FLAG_KEY_FRAME),
                MediaKind.Audio,
                PublishTimeline(0L),
                (clock().instantOf(PlatformTimebase.Unknown, 1_000_000_000L) as? CaptureInstant.At)?.epoch,
            )
        assertIs<OutputDecision.Rejected>(decision, "an unplaceable frame is never published")
    }

    // ---- n: what these oracles reject ---------------------------------------

    @Test
    fun n1_theConversionOracleRefusesAWallClockSubstitute() {
        // The value a callback-arrival wall clock would produce is a different
        // number from the calibrated one, and p1 compares exact values.
        val arrival = UnixMicros(epoch.value + 5_000_000uL)
        assertNotEquals(UnixMicros(epoch.value + 33_333uL), arrival, "an arrival reading cannot satisfy p1")
        assertNotEquals(
            33_333L,
            (1_033_333_000L - calibration.elapsedRealtimeNanos) / 1_000_000L,
            "and a millisecond scale cannot either",
        )
    }

    @Test
    fun n2_theFrameAssociationOracleRefusesTheTimestampsOwnInstant() {
        // Placing every frame at the reported instant would ignore framePosition.
        val timestampInstant = UnixMicros(epoch.value + 1_000_000uL)
        assertNotEquals(UnixMicros(epoch.value + 1_050_000uL), timestampInstant, "a1 compares the associated frame")
        val sendable =
            SendObject(
                payload = MediaBytes(ByteArray(1)),
                isSync = true,
                startsGroup = true,
                presentationTime = MediaMicros(0uL),
                captureTime = timestampInstant,
            )
        assertEquals(timestampInstant, sendable.captureTime, "the value the publish contract would carry")
    }
}
