package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The publish loop's per-output decision, now on the accepted contracts.
 *
 * The encoder is a value source here, exactly as a real `MediaCodec` loop will
 * be: the rows drive outputs through the same pipeline the session runs.
 */
class PublishPipelineRedTest {

    private val epoch = UnixMicros(1_746_104_600_000_000uL)
    private val originNanos = 1_000_000_000L
    private val calibration = ClockCalibration(originNanos, epoch, 120L)

    // A declared Annex-B key frame -- start code, IDR header 0x65, five body
    // bytes -- inside a larger codec buffer, and its declared avc1 payload: the
    // same six-byte NAL unit after its big-endian length. Synthetic framing.
    private val frameBytes = byteArrayOf(0x00, 0x00, 0x00, 0x01, 0x65, 0x11, 0x22, 0x33, 0x44, 0x55)
    private val frameAvc1 = byteArrayOf(0x00, 0x00, 0x00, 0x06, 0x65, 0x11, 0x22, 0x33, 0x44, 0x55)
    private val buffer = byteArrayOf(0x7f, 0x7e) + frameBytes + byteArrayOf(0x00)

    // The validated fixture format's own configuration observation: its SPS
    // then its PPS, each after 00 00 00 01.
    private val configBytes = byteArrayOf(0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1e, 0xab.toByte(), 0xcd.toByte(), 0x00, 0x00, 0x00, 0x01, 0x68, 0xce.toByte(), 0x3c, 0x80.toByte())

    private fun pipeline(kind: MediaKind = MediaKind.Video) =
        PublishPipeline(kind, PublishTimeline(0L), PlatformCaptureClock(calibration), validatedFormat(kind), HOST_LIMITS)

    private fun output(
        ptsUs: Long = 0L,
        flags: Int = CaptureContract.FLAG_KEY_FRAME,
        bytes: ByteArray = buffer,
        offset: Int = 2,
        size: Int = frameBytes.size,
    ) = EncodedOutput(bytes, offset, size, ptsUs, flags)

    @Test
    fun w1_aKeyFrameOutputBecomesTheObjectTheContractsDescribe() {
        val step = pipeline().step(output(), captureNanos = originNanos + 33_333_000L)
        val write = assertIs<PublishStep.Write>(step)
        assertContentEquals(frameAvc1, write.obj.payload.bytes(), "the declared range, copied and converted")
        assertTrue(write.obj.isSync)
        assertTrue(write.obj.startsGroup)
        assertEquals(MediaMicros(0uL), write.obj.presentationTime, "relative to the declared origin")
        assertEquals(UnixMicros(epoch.value + 33_333uL), write.obj.captureTime, "placed on the capture clock")
        assertFalse(write.endsStream)
    }

    @Test
    fun w2_codecConfigurationIsConfiguredNotWritten() {
        val step =
            pipeline().step(
                EncodedOutput(configBytes, 0, configBytes.size, 0L, CaptureContract.FLAG_CODEC_CONFIG),
                captureNanos = originNanos,
            )
        val configure = assertIs<PublishStep.Configure>(step, "csd belongs to the track, never to a write")
        assertContentEquals(configBytes, configure.bytes)
    }

    @Test
    fun w3_anOutputWithNoPlaceableCaptureInstantIsSkippedByName() {
        // A stamp on a clock the calibration does not cover: the accepted clock
        // refuses it, and nothing is published.
        val pipeline = pipeline()
        val skipped = assertIs<PublishStep.Skipped>(pipeline.step(output(), captureNanos = Long.MIN_VALUE))
        assertTrue(skipped.reason.isNotEmpty())
        assertTrue(pipeline.mayEndTrack(), "nothing was admitted")
    }

    @Test
    fun w4_aDeltaBeforeAnySyncFrameIsSkipped() {
        val pipeline = pipeline()
        val skipped =
            assertIs<PublishStep.Skipped>(
                pipeline.step(output(ptsUs = 33_333L, flags = 0), captureNanos = originNanos + 33_333_000L),
                "a video track opens with a sync frame",
            )
        assertTrue(skipped.reason.isNotEmpty())
    }

    @Test
    fun w5_theTerminalEndsTheTrackAndSettlementIsExactlyOnce() {
        val pipeline = pipeline()
        val write = assertIs<PublishStep.Write>(pipeline.step(output(), captureNanos = originNanos))
        assertFalse(pipeline.mayEndTrack(), "the admitted object is still in flight")
        pipeline.accepted(write.obj)
        assertTrue(pipeline.mayEndTrack())

        val end =
            pipeline.step(
                EncodedOutput(ByteArray(0), 0, 0, 66_666L, CaptureContract.FLAG_END_OF_STREAM),
                captureNanos = originNanos + 66_666_000L,
            )
        assertEquals(PublishStep.EndTrack, end)
    }

    @Test
    fun w6_anAbandonedWriteTakesTheAcceptedRecoveryPolicy() {
        val pipeline = pipeline()
        val opener = assertIs<PublishStep.Write>(pipeline.step(output(), captureNanos = originNanos))
        pipeline.accepted(opener.obj)
        val delta =
            assertIs<PublishStep.Write>(
                pipeline.step(output(ptsUs = 33_333L, flags = 0), captureNanos = originNanos + 33_333_000L),
            )
        assertEquals(Recovery.RestartAtSync, pipeline.abandoned(delta.obj))
        assertIs<PublishStep.Skipped>(
            pipeline.step(output(ptsUs = 66_666L, flags = 0), captureNanos = originNanos + 66_666_000L),
            "the broken group cannot continue",
        )
    }

    @Test
    fun w7_theSyntheticEncoderIsAFixtureBehindTheSameSeam() {
        // The generator the sample ships is an EncodedOutputSource like any
        // other: the pipeline cannot tell it from a real codec loop, and the
        // SOURCE owns each output's capture timestamp.
        val source: EncodedOutputSource = SyntheticEncoderOutputs(originNanos, frames = 3, gop = 2)
        val pipeline = pipeline()
        var written = 0
        while (true) {
            val captured = source.next() ?: break
            val step = pipeline.step(captured.output, captured.captureElapsedNanos)
            if (step is PublishStep.Write) {
                written++
                pipeline.accepted(step.obj)
            }
        }
        assertEquals(3, written, "every synthetic frame goes through the same decisions")
    }

    @Test
    fun w8_captureAndMediaTimesEachFollowTheSourceNotALoopCadence() {
        // A real encoder emits on no fixed cadence, and its two times are
        // DIFFERENT facts: the capture instant is a platform timestamp, the
        // presentation time is the encoder's own media clock. This source makes
        // both non-uniform, and neither list is derivable from a loop index.
        val scripted =
            listOf(
                CapturedOutput(output(ptsUs = 0L), originNanos),
                CapturedOutput(output(ptsUs = 40_000L, flags = 0), originNanos + 50_000_000L),
                CapturedOutput(output(ptsUs = 120_000L, flags = 0), originNanos + 150_000_000L),
            )
        var at = 0
        val source = EncodedOutputSource { if (at < scripted.size) scripted[at++] else null }
        val pipeline = pipeline()
        val captureInstants = ArrayList<UnixMicros>()
        val mediaTimes = ArrayList<MediaMicros>()
        while (true) {
            val captured = source.next() ?: break
            val write = assertIs<PublishStep.Write>(pipeline.step(captured.output, captured.captureElapsedNanos))
            captureInstants.add(assertNotNull(write.obj.captureTime))
            mediaTimes.add(write.obj.presentationTime)
            pipeline.accepted(write.obj)
        }
        assertEquals(
            listOf(
                UnixMicros(epoch.value),
                UnixMicros(epoch.value + 50_000uL),
                UnixMicros(epoch.value + 150_000uL),
            ),
            captureInstants,
            "each capture instant is the source's own timestamp on the calibrated clock",
        )
        assertEquals(
            listOf(MediaMicros(0uL), MediaMicros(40_000uL), MediaMicros(120_000uL)),
            mediaTimes,
            "and each media time is the source's own presentation time, read off the written object",
        )
        assertNotEquals(
            captureInstants.map { it.value - epoch.value },
            mediaTimes.map { it.value },
            "the two are different facts here, so neither row can stand in for the other",
        )
    }
}
