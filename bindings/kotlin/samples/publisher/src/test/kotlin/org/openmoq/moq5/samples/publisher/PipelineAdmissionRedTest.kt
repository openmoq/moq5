package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WriteOutcome

/**
 * The converting pipeline's admission and settlement, at its own boundary and
 * through the real drain over the real handoff: the object a video frame is
 * admitted as is the CONVERTED one, that same instance is what is written and
 * settled on every outcome, and a refusal -- a payload, a configuration
 * observation, an allocation failure, a cancellation -- leaves nothing
 * admitted. The validated formats are the fixture validator's; every frame is
 * a declared Annex-B literal and every expected payload a declared literal.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PipelineAdmissionRedTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private val epoch = UnixMicros(1_746_104_600_000_000uL)
    private val originNanos = 1_000_000_000L
    private val calibration = ClockCalibration(originNanos, epoch, 120L)

    /** The fixture video format's SPS and PPS, then an IDR: the frame converts to the IDR alone. */
    private val frame = hex("000000016742001eabcd" + "0000000168ce3c80" + "0000000165888421")
    private val converted = hex("0000000465888421")

    /** A second key frame, 33 333 us later. */
    private val later = hex("000000016588842a")
    private val laterConverted = hex("000000046588842a")

    /** An unknown SPS after the IDR. */
    private val lateUnknown = hex("0000000165888421" + "000000016742001dabcd")

    private fun pipeline(kind: MediaKind = MediaKind.Video, output: AvcOutputAllocator = AvcOutputAllocator.HEAP) =
        PublishPipeline(kind, PublishTimeline(0L), PlatformCaptureClock(calibration), validatedFormat(kind), HOST_LIMITS, output)

    private fun key(bytes: ByteArray, ptsUs: Long = 0L) = EncodedOutput(bytes, 0, bytes.size, ptsUs, CaptureContract.FLAG_KEY_FRAME)

    private fun config(bytes: ByteArray) = EncodedOutput(bytes, 0, bytes.size, 0L, CaptureContract.FLAG_CODEC_CONFIG)

    /** The declared converted object: every field but the payload is what the contract decides for a video key frame. */
    private fun expected(payload: ByteArray, ptsUs: Long) =
        SendObject(
            payload = MediaBytes(payload),
            isSync = true,
            startsGroup = true,
            presentationTime = MediaMicros(ptsUs.toULong()),
            endsGroup = false,
            decodeTime = null,
            captureTime = UnixMicros(epoch.value + ptsUs.toULong()),
            sapType = null,
            cmafProperties = null,
        )

    // ---- a: admission and settlement at the pipeline --------------------------------

    @Test
    fun `a1 the admitted object is the converted one, every other field preserved, and only that instance settles`() {
        val pipeline = pipeline()
        val write = assertIs<PublishStep.Write>(pipeline.step(key(frame), originNanos))
        assertEquals(expected(converted, 0L), write.obj)
        assertFalse(pipeline.mayEndTrack(), "admitted while in flight")
        // An equal object that is not the admitted instance settles nothing.
        assertFailsWith<IllegalStateException> { pipeline.accepted(write.obj.copy()) }
        assertFalse(pipeline.mayEndTrack())
        pipeline.accepted(write.obj)
        assertTrue(pipeline.mayEndTrack(), "settled")
    }

    @Test
    fun `a2 a refused payload leaves no admission, so the next frame is admitted`() {
        val pipeline = pipeline()
        val refused = assertIs<PublishStep.Refused>(pipeline.step(key(lateUnknown), originNanos))
        assertEquals("the Video payload was refused: at 8: a type 7 parameter set that is not in the resolved configuration", refused.reason)
        assertTrue(pipeline.mayEndTrack(), "nothing was admitted")
        val next = assertIs<PublishStep.Write>(pipeline.step(key(later, 33_333L), originNanos + 33_333_000L))
        assertEquals(expected(laterConverted, 33_333L), next.obj)
    }

    @Test
    fun `a3 an allocation failure leaves as itself and admits nothing`() {
        val oom = OutOfMemoryError("the converted payload")
        val pipeline = pipeline(output = AvcOutputAllocator { throw oom })
        assertSame(oom, assertFailsWith<OutOfMemoryError> { pipeline.step(key(frame), originNanos) })
        assertTrue(pipeline.mayEndTrack(), "nothing was admitted")
    }

    @Test
    fun `a4 a cancellation during conversion leaves as itself and admits nothing`() {
        val cancelled = CancellationException("the track was cancelled")
        val pipeline = pipeline(output = AvcOutputAllocator { throw cancelled })
        assertSame(cancelled, assertFailsWith<CancellationException> { pipeline.step(key(frame), originNanos) })
        assertTrue(pipeline.mayEndTrack(), "nothing was admitted")
    }

    @Test
    fun `a5 a configuration observation must be every retained set, in order, byte for byte`() {
        val equal = hex("000000016742001eabcd" + "0000000168ce3c80")
        assertIs<PublishStep.Configure>(pipeline().step(config(equal), originNanos))
        val mismatch = "the Video codec-configuration observation does not match its validated configuration"
        for (bytes in listOf(
            hex("0000000168ce3c80" + "000000016742001eabcd"), // reordered
            hex("000000016742001eabcd" + "0000000168ce3c81"), // same length, last byte changed
            hex("000000016742001eabcd" + "0000000168ce3c80" + "00"), // a trailing zero, not trimmed
            hex("000000016742001eabcd"), // a prefix of it
            hex("0000016742001eabcd" + "0000000168ce3c80"), // a three-byte prefix, not normalized
        )) {
            val pipeline = pipeline()
            assertEquals(mismatch, assertIs<PublishStep.Refused>(pipeline.step(config(bytes), originNanos)).reason)
            assertTrue(pipeline.mayEndTrack())
        }
    }

    @Test
    fun `a6 an audio observation must be the AudioSpecificConfig, and audio media is admitted unchanged`() {
        assertIs<PublishStep.Configure>(pipeline(MediaKind.Audio).step(config(FIXTURE_ASC.copyOf()), originNanos))
        assertEquals(
            "the Audio codec-configuration observation does not match its validated configuration",
            assertIs<PublishStep.Refused>(pipeline(MediaKind.Audio).step(config(hex("1210")), originNanos)).reason,
        )
        val unit = hex("21102a")
        val write = assertIs<PublishStep.Write>(pipeline(MediaKind.Audio).step(EncodedOutput(unit, 0, unit.size, 0L, 0), originNanos))
        assertContentEquals(unit, write.obj.payload.bytes(), "audio bytes are not converted")
    }

    // ---- d: through the real drain, over the real handoff ---------------------------

    private class Lane(val kind: MediaKind, val pipeline: PublishPipeline) {
        val buffers = HostBuffers()
        val releases = HostReleases()
        val ledger = BoundedCaptureLedger(kind, GENERATION, capacity = 16)
        val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, HostFaults(), CAPACITY, HOST_MAX_OUTPUT_BYTES)

        fun queue(index: Int, bytes: ByteArray, pts: Long, flags: Int) {
            buffers.put(index, ByteBuffer.wrap(bytes))
            if (flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && bytes.isNotEmpty()) {
                assertEquals(ObserveOutcome.Recorded, ledger.observe(GENERATION, kind, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
            }
            assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(index, 0, bytes.size, pts, flags)))
        }
    }

    /** Records each object it is handed and whether the pipeline held it admitted at that moment; answers or raises as scripted. */
    private class AdmissionSink(private val pipeline: PublishPipeline, private val answer: () -> WriteOutcome) : TrackSink {
        val written = mutableListOf<SendObject>()
        val admittedAtWrite = mutableListOf<Boolean>()
        val admittedAtEnd = mutableListOf<Boolean>()

        override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome {
            written += obj
            admittedAtWrite += !pipeline.mayEndTrack()
            return answer()
        }

        override suspend fun endTrack(timeout: Duration): WriteOutcome {
            admittedAtEnd += !pipeline.mayEndTrack()
            return WriteOutcome.Accepted
        }
    }

    private fun lane(): Lane =
        Lane(MediaKind.Video, PublishPipeline(MediaKind.Video, PublishTimeline(0L), PlatformCaptureClock(DRAIN_CALIBRATION), validatedVideo(), HOST_LIMITS))

    @Test
    fun `d1 accepted - the converted object is written while admitted and settled before the track ends`() = runTest {
        val lane = lane()
        val sink = AdmissionSink(lane.pipeline) { WriteOutcome.Accepted }
        lane.queue(0, frame, 33_333L, CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_END_OF_STREAM)
        val drained = drainTrack(MediaKind.Video, lane.handoff, lane.pipeline, sink, WRITE_TIMEOUT)
        assertEquals(TrackDrain.Ended(written = 1, refused = 0, skipped = 0), drained)
        assertContentEquals(converted, sink.written.single().payload.bytes())
        assertEquals(listOf(true), sink.admittedAtWrite, "written while admitted")
        assertEquals(listOf(false), sink.admittedAtEnd, "settled before the end")
        // The settlement already consumed that exact instance: settling it again is refused.
        assertFailsWith<IllegalStateException> { lane.pipeline.accepted(sink.written.single()) }
    }

    @Test
    fun `d2 WouldBlock - the converted object is abandoned, and nothing is left admitted`() = runTest {
        val lane = lane()
        val sink = AdmissionSink(lane.pipeline) { WriteOutcome.WouldBlock }
        lane.queue(0, frame, 33_333L, CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_END_OF_STREAM)
        val drained = drainTrack(MediaKind.Video, lane.handoff, lane.pipeline, sink, WRITE_TIMEOUT)
        assertEquals(TrackDrain.Ended(written = 0, refused = 1, skipped = 0), drained)
        assertContentEquals(converted, sink.written.single().payload.bytes())
        assertEquals(listOf(true), sink.admittedAtWrite)
        assertEquals(listOf(false), sink.admittedAtEnd)
    }

    @Test
    fun `d3 a write that raises settles the admission and keeps its failure`() = runTest {
        val lane = lane()
        val failure = IllegalStateException("the sender failed")
        val sink = AdmissionSink(lane.pipeline) { throw failure }
        lane.queue(0, frame, 33_333L, CaptureContract.FLAG_KEY_FRAME)
        assertSame(failure, assertFailsWith<IllegalStateException> { drainTrack(MediaKind.Video, lane.handoff, lane.pipeline, sink, WRITE_TIMEOUT) })
        assertContentEquals(converted, sink.written.single().payload.bytes())
        assertEquals(listOf(true), sink.admittedAtWrite)
        assertTrue(lane.pipeline.mayEndTrack(), "the admission was settled on the failure path")
    }

    @Test
    fun `d4 a refused payload ends the drain unfinished, with no write, no end and nothing admitted`() = runTest {
        val lane = lane()
        val sink = AdmissionSink(lane.pipeline) { WriteOutcome.Accepted }
        lane.queue(0, lateUnknown, 33_333L, CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_END_OF_STREAM)
        val drained = drainTrack(MediaKind.Video, lane.handoff, lane.pipeline, sink, WRITE_TIMEOUT)
        assertEquals(
            TrackDrain.Unfinished("the Video payload was refused: at 8: a type 7 parameter set that is not in the resolved configuration", 0, 0, 0),
            drained,
        )
        assertEquals(emptyList(), sink.written)
        assertEquals(emptyList(), sink.admittedAtEnd, "a refused terminal payload manufactures no end")
        assertTrue(lane.pipeline.mayEndTrack())
    }

    private companion object {
        val WRITE_TIMEOUT: Duration = 1.seconds
        val DRAIN_CALIBRATION = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L)
    }
}
