package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.coroutineContext
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WriteOutcome

/**
 * The live per-track drain, against the real handoff, the real ledger and the
 * accepted publish pipeline. The sender is scripted; nothing here opens a
 * socket, a camera or a codec.
 *
 * The sink records ONE ordered inventory of everything it was asked to do --
 * which track asked, which object, with which timeout, and what the pipeline
 * said about its admission at the moment the track was ended. Rows compare that
 * whole inventory, so an extra call, a missing one or a reordered pair fails;
 * expected objects are DECLARED here field by field, never re-derived by
 * running the pipeline a second time.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class TrackDrainRedTest {

    /** Everything the sender was asked to do, in the order it was asked. */
    private sealed interface SinkCall {
        data class Wrote(val kind: MediaKind, val obj: SendObject, val timeout: Duration) : SinkCall

        /** [mayEndTrack] is read from the pipeline AT THE END CALL, not afterwards. */
        data class Ended(val kind: MediaKind, val timeout: Duration, val mayEndTrack: Boolean) : SinkCall
    }

    /**
     * A scripted sender for ONE track. It records and answers; it implements no
     * part of the loop. [beforeWrite] runs before the call is recorded, so a row
     * can make the boundary itself interesting.
     */
    private class RecordingSink(
        private val kind: MediaKind,
        private val calls: MutableList<SinkCall>,
        private val mayEndTrack: () -> Boolean,
        private val writes: ArrayDeque<WriteOutcome> = ArrayDeque(),
        private val ends: ArrayDeque<WriteOutcome> = ArrayDeque(),
        private val beforeWrite: (suspend (Int) -> Unit)? = null,
    ) : TrackSink {
        private var attempts = 0

        override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome {
            beforeWrite?.invoke(attempts++)
            calls.add(SinkCall.Wrote(kind, obj, timeout))
            return writes.removeFirstOrNull() ?: WriteOutcome.Accepted
        }

        override suspend fun endTrack(timeout: Duration): WriteOutcome {
            calls.add(SinkCall.Ended(kind, timeout, mayEndTrack()))
            return ends.removeFirstOrNull() ?: WriteOutcome.Accepted
        }

        fun scriptWrites(vararg outcomes: WriteOutcome) = apply { writes.addAll(outcomes) }

        fun scriptEnds(vararg outcomes: WriteOutcome) = apply { ends.addAll(outcomes) }
    }

    private class Rig(val kind: MediaKind = MediaKind.Video) {
        val buffers = HostBuffers()
        val releases = HostReleases()
        val ledger = BoundedCaptureLedger(kind, GENERATION, capacity = 16)
        val faults = HostFaults()
        val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val clock = PlatformCaptureClock(ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L))
        val pipeline = PublishPipeline(kind, PublishTimeline(0L), clock, validatedFormat(kind), HOST_LIMITS)

        /** One encoder callback: the platform's bytes and the instant it captured them. */
        fun queue(index: Int, bytes: ByteArray, pts: Long, flags: Int = CaptureContract.FLAG_KEY_FRAME): HandoffOutcome {
            buffers.put(index, ByteBuffer.wrap(bytes))
            if (flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && bytes.isNotEmpty()) {
                assertEquals(ObserveOutcome.Recorded, ledger.observe(GENERATION, kind, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
            }
            return handoff.onOutput(GENERATION, CodecOutput(index, 0, bytes.size, pts, flags))
        }

        /** The empty terminal. */
        fun queueEnd(index: Int): HandoffOutcome {
            buffers.put(index, ByteBuffer.allocate(0))
            return handoff.onOutput(GENERATION, CodecOutput(index, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM))
        }

        /** A final MEDIA payload that also carries end-of-stream. */
        fun queueFinal(index: Int, bytes: ByteArray, pts: Long): HandoffOutcome =
            queue(index, bytes, pts, CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_END_OF_STREAM)
    }

    private fun bytes(tag: Int, size: Int) = ByteArray(size) { ((tag shl 4) + it).toByte() }

    /** A declared Annex-B key frame: `00 00 00 01`, an IDR header 0x65, then [size] tagged bytes. Synthetic framing, not a picture. */
    private fun videoFrame(tag: Int, size: Int) = byteArrayOf(0, 0, 0, 1, 0x65) + bytes(tag, size)

    /** Its declared avc1 payload: the same NAL unit after its big-endian length, one header byte plus [size]. */
    private fun videoAvc1(tag: Int, size: Int) = byteArrayOf(0, 0, 0, (size + 1).toByte(), 0x65) + bytes(tag, size)

    /**
     * Every field of one recorded write, against values DECLARED by the row.
     * A video key frame is independently decodable and opens a group; an audio
     * access unit is independently decodable and opens none.
     */
    private fun assertWrote(
        call: SinkCall,
        kind: MediaKind,
        payload: ByteArray,
        presentationTime: ULong,
        captureTime: ULong,
        isSync: Boolean,
        startsGroup: Boolean,
        at: String,
    ) {
        val wrote = assertIs<SinkCall.Wrote>(call, "$at: this call is a write")
        assertEquals(kind, wrote.kind, "$at: written to its OWN track's sender")
        assertEquals(WRITE_TIMEOUT, wrote.timeout, "$at: with the declared timeout")
        val obj = wrote.obj
        assertContentEquals(payload, obj.payload.bytes(), "$at: the exact captured bytes")
        assertEquals(isSync, obj.isSync, "$at: isSync")
        assertEquals(startsGroup, obj.startsGroup, "$at: startsGroup")
        assertEquals(false, obj.endsGroup, "$at: endsGroup")
        assertEquals(MediaMicros(presentationTime), obj.presentationTime, "$at: presentationTime")
        assertNull(obj.decodeTime, "$at: this sample publishes no decode time")
        assertEquals(UnixMicros(captureTime), obj.captureTime, "$at: the instant the ledger claimed")
        assertNull(obj.sapType, "$at: no SAP type")
        assertNull(obj.cmafProperties, "$at: no CMAF properties")
    }

    private fun assertEnded(call: SinkCall, kind: MediaKind, at: String) {
        val ended = assertIs<SinkCall.Ended>(call, "$at: this call is the track's end")
        assertEquals(kind, ended.kind, "$at: ended on its OWN track")
        assertEquals(WRITE_TIMEOUT, ended.timeout, "$at: with the declared timeout")
        assertTrue(ended.mayEndTrack, "$at: nothing was still admitted when the track was ended")
    }

    // ---- the rows --------------------------------------------------------------

    /** An output queued before the drain ever looked is still drained and written. */
    @Test
    fun d1_outputQueuedBeforeTheDrainStartsIsWritten() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val first = videoFrame(1, 6)
        assertEquals(HandoffOutcome.Queued, rig.queue(0, first, 33_333L))
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(1))
        val outcome = assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() }, "the drain finished")
        assertEquals(TrackDrain.Ended(written = 1, refused = 0, skipped = 0), outcome)
        assertEquals(2, calls.size, "exactly one write and one end: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(1, 6), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "the only frame")
        assertEnded(calls[1], MediaKind.Video, at = "the end")
    }

    /**
     * A source whose first empty answer delivers an output: the callback lands
     * exactly between the empty check and the suspension. The wake can only be
     * there to catch it if the signal was installed FIRST.
     */
    private class DeliversOnFirstEmpty(private val inner: CaptureHandoff, private val deliver: () -> Unit) : CaptureHandoff by inner {
        private var armed = true

        override fun next(): CapturedOutput? {
            val item = inner.next()
            if (item == null && armed) {
                armed = false
                deliver()
            }
            return item
        }
    }

    @Test
    fun d2_anOutputArrivingInTheSuspendWindowIsNotLost() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val payload = videoFrame(2, 5)
        val racing =
            DeliversOnFirstEmpty(rig.handoff) {
                assertEquals(HandoffOutcome.Queued, rig.queue(0, payload, 33_333L))
                assertEquals(HandoffOutcome.Queued, rig.queueEnd(1))
            }
        val drain = async { drainTrack(rig.kind, racing, rig.pipeline, sink, WRITE_TIMEOUT) }
        val outcome =
            assertNotNull(
                withTimeoutOrNull(BOUNDED) { drain.await() },
                "an output delivered between the empty check and the suspension must not be lost",
            )
        assertEquals(TrackDrain.Ended(written = 1, refused = 0, skipped = 0), outcome)
        assertEquals(2, calls.size, "one write, one end: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(2, 5), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "the raced frame")
        assertEnded(calls[1], MediaKind.Video, at = "the end")
    }

    /** One wake, several payloads and the terminal behind them: all of them are written, in order. */
    @Test
    fun d3_oneWakeDrainsEveryOutputQueuedBehindIt() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        val one = videoFrame(1, 6)
        val two = videoFrame(2, 4)
        assertEquals(HandoffOutcome.Queued, rig.queue(0, one, 33_333L))
        assertEquals(HandoffOutcome.Queued, rig.queue(1, two, 66_666L))
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(2))
        val outcome =
            assertNotNull(
                withTimeoutOrNull(BOUNDED) { drain.await() },
                "one wake must drain every output queued behind it",
            )
        assertEquals(TrackDrain.Ended(written = 2, refused = 0, skipped = 0), outcome)
        assertEquals(3, calls.size, "two writes then one end: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(1, 6), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "the first frame")
        assertWrote(calls[1], MediaKind.Video, videoAvc1(2, 4), 66_666uL, EPOCH + 66_666uL, isSync = true, startsGroup = true, at = "the second frame")
        assertEnded(calls[2], MediaKind.Video, at = "the end")
    }

    /** Idle, active, idle again, then the terminal. */
    @Test
    fun d4_theDrainSurvivesEveryIdlePeriod() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(emptyList(), calls, "an idle track asks the sender for nothing")
        assertEquals(HandoffOutcome.Queued, rig.queue(0, videoFrame(1, 6), 33_333L))
        runCurrent()
        assertEquals(1, calls.size, "an empty queue is not the end: the drain was still there for this one")
        assertTrue(drain.isActive, "and it is still running, idle again")
        assertEquals(HandoffOutcome.Queued, rig.queue(1, videoFrame(2, 6), 66_666L))
        runCurrent()
        assertEquals(2, calls.size)
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(2))
        assertEquals(TrackDrain.Ended(written = 2, refused = 0, skipped = 0), assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() }))
        assertEquals(3, calls.size, "two writes and one end, nothing else: $calls")
        assertEnded(calls[2], MediaKind.Video, at = "the end")
    }

    /** A final payload that also carries the terminal is written and settled BEFORE the track ends. */
    @Test
    fun d5_aFinalMediaPayloadIsWrittenBeforeTheTrackEnds() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val last = videoFrame(3, 7)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queueFinal(0, last, 33_333L))
        val outcome = assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() })
        assertEquals(TrackDrain.Ended(written = 1, refused = 0, skipped = 0), outcome)
        assertEquals(2, calls.size, "the write, then the end: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(3, 7), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "the final media")
        // The ORDER is the point: the end is second, and at that moment the
        // pipeline held no admitted write.
        assertEnded(calls[1], MediaKind.Video, at = "the end after the final media")
    }

    /** The same helper, on the audio track: an access unit is independent and opens no group. */
    @Test
    fun d6_anAudioTrackRunsTheSameHelperWithItsOwnFlags() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig(MediaKind.Audio)
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val unit = bytes(4, 3)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queue(0, unit, 21_333L, flags = 0))
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(1))
        assertEquals(TrackDrain.Ended(written = 1, refused = 0, skipped = 0), assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() }))
        assertEquals(2, calls.size, "one write, one end: $calls")
        assertWrote(calls[0], MediaKind.Audio, unit, 21_333uL, EPOCH + 21_333uL, isSync = true, startsGroup = false, at = "the audio unit")
        assertEnded(calls[1], MediaKind.Audio, at = "the audio end")
    }

    /** A refusal is counted as a refusal, settled once, and is not a written object. */
    @Test
    fun d7_aRefusedWriteIsCountedAndSettledOnce() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack).scriptWrites(WriteOutcome.WouldBlock, WriteOutcome.Accepted)
        val refusedPayload = videoFrame(1, 6)
        val acceptedPayload = videoFrame(2, 6)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queue(0, refusedPayload, 33_333L))
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queue(1, acceptedPayload, 66_666L))
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(2))
        val outcome = assertIs<TrackDrain.Ended>(assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() }))
        assertEquals(1, outcome.written, "one write resolved Accepted")
        assertEquals(1, outcome.refused, "and one resolved WouldBlock")
        assertEquals(3, calls.size, "both attempts, then the end: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(1, 6), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "the refused attempt")
        assertWrote(calls[1], MediaKind.Video, videoAvc1(2, 6), 66_666uL, EPOCH + 66_666uL, isSync = true, startsGroup = true, at = "the accepted attempt")
        assertEnded(calls[2], MediaKind.Video, at = "the end")
    }

    /** An endTrack the sender refuses is not a successful end. */
    @Test
    fun d8_aRefusedEndTrackIsUnfinished() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack).scriptEnds(WriteOutcome.WouldBlock)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queue(0, videoFrame(1, 6), 33_333L))
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(1))
        val outcome = assertIs<TrackDrain.Unfinished>(assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() }))
        assertTrue(outcome.reason.contains("endTrack"), "named by what refused it: ${outcome.reason}")
        assertEquals(1, outcome.written, "the media before it was still written")
        assertEquals(2, calls.size, "one write and ONE end attempt, not retried into success: $calls")
        assertEnded(calls[1], MediaKind.Video, at = "the refused end")
    }

    /** A source that ends WITHOUT a terminal is unfinished, whatever `ended` says. */
    @Test
    fun d9_aSourceThatEndsWithoutATerminalIsUnfinished() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queue(0, videoFrame(1, 6), 33_333L))
        runCurrent()
        // The user stopped: the track is closed, and no end-of-stream was ever
        // produced by the encoder.
        rig.handoff.close(CloseMode.Failed)
        val outcome = assertIs<TrackDrain.Unfinished>(assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() }))
        assertTrue(outcome.reason.contains("without"), "named: ${outcome.reason}")
        assertEquals(1, outcome.written, "what was written before it stands")
        assertEquals(1, calls.size, "a track that never ended is not ended on the sender: $calls")
    }

    /** A write that throws leaves by identity, settles this drain's admission, and invents no receipt. */
    @Test
    fun d10_aWriteThatThrowsSettlesTheAdmissionAndKeepsItsFailure() = runTest {
        for (raised in listOf<Throwable>(IllegalStateException("the sender failed"), AssertionError("an Error inside write"))) {
            val calls = mutableListOf<SinkCall>()
            val rig = Rig()
            val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack, beforeWrite = { throw raised })
            // The output is queued before the drain runs, so the write -- and
            // its failure -- happen on THIS coroutine: the identity that
            // arrives is the drain's own, not one recovered across an await.
            assertEquals(HandoffOutcome.Queued, rig.queue(0, videoFrame(1, 6), 33_333L))
            val thrown = runCatching { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }.exceptionOrNull()
            assertSame(raised, thrown, "what the sender threw is what leaves")
            assertTrue(rig.pipeline.mayEndTrack(), "the local admission was settled exactly once")
            assertEquals(emptyList(), calls, "nothing was recorded as written, and the track was not ended")
        }
    }

    /** Cancelling an idle drain is cancellation, not an outcome value. */
    @Test
    fun d11_cancellingAnIdleDrainStaysCancellation() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        val cause = CancellationException("the coordinator stopped this track")
        drain.cancel(cause)
        val thrown = assertFailsWith<CancellationException> { drain.await() }
        assertTrue(
            thrown.message.orEmpty().contains("the coordinator stopped this track"),
            "cancellation leaves as cancellation, carrying its cause: ${thrown.message}",
        )
        assertEquals(emptyList(), calls, "a cancelled drain asks the sender for nothing")
        // A callback that arrives after the drain is gone is harmless: the wake
        // has nowhere to go and nothing throws.
        assertEquals(HandoffOutcome.Queued, rig.queue(0, videoFrame(1, 6), 33_333L))
        assertEquals(emptyList(), calls)
    }

    /** Nothing is ever written after the terminal, whatever arrives behind it. */
    @Test
    fun d12_nothingIsWrittenAfterTheTerminal() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val sink = RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack)
        val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
        runCurrent()
        assertEquals(HandoffOutcome.Queued, rig.queue(0, videoFrame(1, 6), 33_333L))
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(1))
        assertEquals(TrackDrain.Ended(written = 1, refused = 0, skipped = 0), assertNotNull(withTimeoutOrNull(BOUNDED) { drain.await() }))
        // A callback that arrives after the terminal was drained is still
        // ADMITTED by the handoff -- the drain does not own the track and does
        // not close it -- but there is no drain left to publish it.
        assertEquals(HandoffOutcome.Queued, rig.queue(2, videoFrame(9, 4), 99_999L))
        assertEquals(2, calls.size, "still exactly one write and one end: $calls")
    }

    // ---- cancellation at the boundaries a suspend call need not suspend at ----

    /**
     * The write RESOLVES at the same boundary the drain is cancelled at. Its
     * settlement stands -- it really was answered -- but the terminal is a NEW
     * operation, and a cancelled drain does not start one.
     */
    @Test
    fun c1_cancellationAtAResolvedFinalWriteDoesNotEndTheTrack() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val job = Job(coroutineContext[Job])
        val sink =
            RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack, beforeWrite = { job.cancel(CancellationException("stopped at the write")) })
        val last = videoFrame(3, 7)
        assertEquals(HandoffOutcome.Queued, rig.queueFinal(0, last, 33_333L))
        val outcome =
            supervisorScope {
                val drain = async(job) { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
                runCurrent()
                runCatching { drain.await() }
            }
        assertIs<CancellationException>(outcome.exceptionOrNull(), "the drain ends as cancellation, not as an outcome")
        assertEquals(1, calls.size, "the write that resolved is the ONLY call: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(3, 7), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "the resolved write")
        assertTrue(rig.pipeline.mayEndTrack(), "its admission was settled")
    }

    /** The same boundary with a queued successor: the successor is never written. */
    @Test
    fun c2_cancellationAtAResolvedWriteDoesNotTakeTheNextOutput() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val job = Job(coroutineContext[Job])
        val sink =
            RecordingSink(rig.kind, calls, rig.pipeline::mayEndTrack, beforeWrite = { at -> if (at == 0) job.cancel(CancellationException("stopped at the write")) })
        val first = videoFrame(1, 6)
        assertEquals(HandoffOutcome.Queued, rig.queue(0, first, 33_333L))
        assertEquals(HandoffOutcome.Queued, rig.queue(1, videoFrame(2, 6), 66_666L))
        assertEquals(HandoffOutcome.Queued, rig.queueEnd(2))
        val outcome =
            supervisorScope {
                val drain = async(job) { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
                runCurrent()
                runCatching { drain.await() }
            }
        assertIs<CancellationException>(outcome.exceptionOrNull())
        assertEquals(1, calls.size, "only the write that had already been started: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(1, 6), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "the resolved write")
        assertTrue(rig.pipeline.mayEndTrack(), "and its admission was settled")
    }

    /** Cancellation while a write is really suspended: outstanding before, settled after. */
    @Test
    fun c3_cancellationWhileAWriteIsSuspendedSettlesTheAdmission() = runTest {
        val calls = mutableListOf<SinkCall>()
        val rig = Rig()
        val inWrite = CompletableDeferred<Unit>()
        val held = CompletableDeferred<Unit>()
        val sink =
            RecordingSink(
                rig.kind,
                calls,
                rig.pipeline::mayEndTrack,
                beforeWrite = {
                    inWrite.complete(Unit)
                    held.await()
                },
            )
        assertEquals(HandoffOutcome.Queued, rig.queue(0, videoFrame(1, 6), 33_333L))
        val outcome =
            supervisorScope {
                val drain = async { drainTrack(rig.kind, rig.handoff, rig.pipeline, sink, WRITE_TIMEOUT) }
                runCurrent()
                inWrite.await()
                assertTrue(!rig.pipeline.mayEndTrack(), "the admission is outstanding while the write is suspended")
                drain.cancel(CancellationException("stopped mid-write"))
                runCurrent()
                runCatching { drain.await() }
            }
        assertIs<CancellationException>(outcome.exceptionOrNull())
        assertTrue(rig.pipeline.mayEndTrack(), "and it is settled once the cancelled write unwinds")
        assertEquals(emptyList(), calls, "the write never resolved, so nothing was recorded: $calls")
    }

    private companion object {
        const val GENERATION = 1L
        const val CAPACITY = 4
        const val BASE_NANOS = 1_000_000_000L
        const val EPOCH = 1_700_000_000_000_000uL
        val WRITE_TIMEOUT = 1.seconds
        val BOUNDED = 30.seconds
    }
}
