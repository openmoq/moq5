package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WriteOutcome

/**
 * The finite demonstration's publish loop, [publishFinite] -- the one
 * `BroadcastSession` runs -- over a counting source, the converting pipeline
 * of the validated fixture format, and a scripted sink. The terminal and
 * settlement rules are the live drain's: a nonempty terminal is written and
 * settled, then the track ends and nothing after it is read; a write that
 * raises, or a cancellation after an answer, still settles the admission and
 * starts no new work; a refused `endTrack` is not a successful end.
 *
 * Every frame is the declared Annex-B [VIDEO_ANNEXB] and every expected write
 * its declared [VIDEO_AVC1]. `c` rows are controls that hold before and after.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class FiniteDemoRedTest {

    private sealed interface Call {
        data class Wrote(val payload: List<Byte>, val admitted: Boolean) : Call

        data class Ended(val mayEndTrack: Boolean) : Call
    }

    /** The outputs in order; every read is counted, including one that finds nothing left. */
    private class CountingSource(private val outputs: List<CapturedOutput>) : EncodedOutputSource {
        var reads = 0

        override fun next(): CapturedOutput? = outputs.getOrNull(reads++)
    }

    /** Answers each write by its script, in order; records every call with the pipeline's admission state at that moment. */
    private class ScriptedSink(
        private val pipeline: PublishPipeline,
        private val writes: List<suspend () -> WriteOutcome>,
        private val end: WriteOutcome = WriteOutcome.Accepted,
    ) : TrackSink {
        val calls = mutableListOf<Call>()
        private var at = 0

        override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome {
            calls += Call.Wrote(obj.payload.bytes().toList(), !pipeline.mayEndTrack())
            return writes[at++]()
        }

        override suspend fun endTrack(timeout: Duration): WriteOutcome {
            calls += Call.Ended(pipeline.mayEndTrack())
            return end
        }
    }

    private fun pipeline() =
        PublishPipeline(
            MediaKind.Video,
            PublishTimeline(0L),
            PlatformCaptureClock(ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L)),
            validatedVideo(),
            HOST_LIMITS,
        )

    private fun media(pts: Long, flags: Int = CaptureContract.FLAG_KEY_FRAME, bytes: ByteArray = VIDEO_ANNEXB) =
        CapturedOutput(EncodedOutput(bytes.copyOf(), 0, bytes.size, pts, flags), BASE_NANOS + pts * 1_000L)

    private fun emptyEnd() = CapturedOutput(EncodedOutput(ByteArray(0), 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM), 0L)

    private val terminalMedia = CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_END_OF_STREAM

    /** An unknown SPS after an IDR: a refused frame. */
    private val lateUnknown = byteArrayOf(0, 0, 0, 1, 0x65, 0x88.toByte(), 0x84.toByte(), 0x21, 0, 0, 0, 1, 0x67, 0x42, 0x00, 0x1d, 0xab.toByte(), 0xcd.toByte())

    private val written = Call.Wrote(VIDEO_AVC1.toList(), admitted = true)

    private val accept: suspend () -> WriteOutcome = { WriteOutcome.Accepted }

    private suspend fun run(source: CountingSource, pipeline: PublishPipeline, sink: ScriptedSink): TrackDrain =
        publishFinite(MediaKind.Video, source, pipeline, sink, WRITE_TIMEOUT, {}) { _, _ -> }

    // ---- c: controls ------------------------------------------------------------

    @Test
    fun `c1 an empty terminal ends the track and nothing after it is read`() = runTest {
        val source = CountingSource(listOf(media(33_333L), emptyEnd(), media(66_666L)))
        val pipeline = pipeline()
        val sink = ScriptedSink(pipeline, listOf(accept))
        assertEquals(TrackDrain.Ended(1, 0, 0), run(source, pipeline, sink))
        assertEquals(listOf<Call>(written, Call.Ended(mayEndTrack = true)), sink.calls)
        assertEquals(2, source.reads, "the output after the terminal is never read")
    }

    @Test
    fun `c2 a finite source that runs out ends the track after every write settled`() = runTest {
        val source = CountingSource(listOf(media(33_333L), media(66_666L)))
        val pipeline = pipeline()
        val sink = ScriptedSink(pipeline, listOf(accept, accept))
        assertEquals(TrackDrain.Ended(2, 0, 0), run(source, pipeline, sink))
        assertEquals(listOf<Call>(written, written, Call.Ended(mayEndTrack = true)), sink.calls)
        assertEquals(3, source.reads, "two outputs, then the read that finds none")
    }

    @Test
    fun `c3 a refused payload stops the track - no write, no end, nothing more read`() = runTest {
        val source = CountingSource(listOf(media(33_333L, bytes = lateUnknown), media(66_666L)))
        val pipeline = pipeline()
        val sink = ScriptedSink(pipeline, emptyList())
        assertEquals(
            TrackDrain.Unfinished("the Video payload was refused: at 8: a type 7 parameter set that is not in the resolved configuration", 0, 0, 0),
            run(source, pipeline, sink),
        )
        assertEquals(emptyList(), sink.calls)
        assertEquals(1, source.reads)
        assertTrue(pipeline.mayEndTrack())
    }

    // ---- f: the terminal and settlement obligations ------------------------------------

    @Test
    fun `f1 nonempty terminal media is written and settled, then the track ends and nothing after it is read`() = runTest {
        val source = CountingSource(listOf(media(33_333L, terminalMedia), media(66_666L)))
        val pipeline = pipeline()
        val sink = ScriptedSink(pipeline, listOf(accept, accept))
        assertEquals(TrackDrain.Ended(1, 0, 0), run(source, pipeline, sink))
        assertEquals(listOf<Call>(written, Call.Ended(mayEndTrack = true)), sink.calls)
        assertEquals(1, source.reads, "the output after the terminal is never read")
    }

    @Test
    fun `f2 a WouldBlock on terminal media abandons that write before the terminal, and nothing after it is read`() = runTest {
        val source = CountingSource(listOf(media(33_333L, terminalMedia), media(66_666L)))
        val pipeline = pipeline()
        val sink = ScriptedSink(pipeline, listOf({ WriteOutcome.WouldBlock }, accept))
        assertEquals(TrackDrain.Ended(0, 1, 0), run(source, pipeline, sink))
        assertEquals(listOf<Call>(written, Call.Ended(mayEndTrack = true)), sink.calls, "abandoned, not sent, before the end")
        assertEquals(1, source.reads)
    }

    private suspend fun raisingWriteSettles(raised: Throwable) {
        val source = CountingSource(listOf(media(33_333L), media(66_666L)))
        val pipeline = pipeline()
        val sink = ScriptedSink(pipeline, listOf({ throw raised }, accept))
        val thrown = runCatching { run(source, pipeline, sink) }.exceptionOrNull()
        assertSame(raised, thrown, "the same instance leaves")
        assertTrue(pipeline.mayEndTrack(), "the admission was settled on the failure path")
        assertEquals(listOf<Call>(written), sink.calls, "no end")
        assertEquals(1, source.reads, "no later read")
    }

    @Test
    fun `f3 a write that raises an exception settles the admission and starts nothing more`() = runTest {
        raisingWriteSettles(IllegalStateException("the sender failed"))
    }

    @Test
    fun `f3b a write that raises an error settles the admission and starts nothing more`() = runTest {
        raisingWriteSettles(AssertionError("an Error inside write"))
    }

    @Test
    fun `f3c a write that is cancelled settles the admission and starts nothing more`() = runTest {
        raisingWriteSettles(CancellationException("the write was cancelled"))
    }

    @Test
    fun `f4 a cancellation after a committed answer settles that answer, then starts no new work`() = runTest {
        val source = CountingSource(listOf(media(33_333L), media(66_666L)))
        val pipeline = pipeline()
        // The answer is committed; the loop's own job is cancelled as it returns.
        val sink = ScriptedSink(pipeline, listOf({ currentCoroutineContext().job.cancel(); WriteOutcome.Accepted }, accept))
        var outcome: Result<TrackDrain>? = null
        launch { outcome = runCatching { run(source, pipeline, sink) } }.join()
        assertIs<CancellationException>(outcome?.exceptionOrNull(), "the loop leaves as cancellation: $outcome")
        assertTrue(pipeline.mayEndTrack(), "the committed answer was settled")
        assertEquals(listOf<Call>(written), sink.calls, "no further write and no end")
        assertEquals(1, source.reads, "no later read")
    }

    @Test
    fun `f4b a cancellation after a committed answer to TERMINAL media settles it and starts no endTrack`() = runTest {
        val source = CountingSource(listOf(media(33_333L, terminalMedia), media(66_666L)))
        val pipeline = pipeline()
        // The terminal's answer is committed; the loop's own job is cancelled as it returns.
        val sink = ScriptedSink(pipeline, listOf({ currentCoroutineContext().job.cancel(); WriteOutcome.Accepted }, accept))
        var outcome: Result<TrackDrain>? = null
        launch { outcome = runCatching { run(source, pipeline, sink) } }.join()
        assertIs<CancellationException>(outcome?.exceptionOrNull(), "the loop leaves as cancellation, not as an end: $outcome")
        assertTrue(pipeline.mayEndTrack(), "the committed answer was settled")
        assertEquals(listOf<Call>(written), sink.calls, "one write and NO endTrack")
        assertEquals(1, source.reads, "no later read")
    }

    // ---- l: the loop inside the real broadcast lifetime ------------------------------

    /**
     * The REAL [publishFinite] run as the REAL generic [broadcastLifetime]'s
     * body, with scripted connect/attach values and close callbacks: one
     * ordered inventory of everything that happened, from connect to the
     * closes. Not `BroadcastSession.publish` itself -- its complete/drain
     * branch needs a sender and stays source-reviewed.
     */
    private suspend fun raisingWriteInsideTheLifetime(raised: Throwable) {
        val effects = mutableListOf<String>()
        val source = CountingSource(listOf(media(33_333L), media(66_666L)))
        val pipeline = pipeline()
        val sink =
            object : TrackSink {
                override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome {
                    effects += "write ${obj.payload.size}b admitted=${!pipeline.mayEndTrack()}"
                    throw raised
                }

                override suspend fun endTrack(timeout: Duration): WriteOutcome {
                    effects += "end"
                    return WriteOutcome.Accepted
                }
            }
        val thrown =
            runCatching {
                broadcastLifetime(
                    connect = { effects += "connect"; "endpoint" },
                    describe = { effects += "describe $it" },
                    attach = { effects += "attach $it"; "child" },
                    run = { endpoint, child ->
                        effects += "run $endpoint $child"
                        publishFinite(MediaKind.Video, source, pipeline, sink, WRITE_TIMEOUT, {}) { _, _ -> }
                    },
                    closeChild = { effects += "close $it" },
                    closeEndpoint = { effects += "close $it" },
                )
            }.exceptionOrNull()
        assertSame(raised, thrown, "the same instance leaves the lifetime")
        assertEquals(emptyList(), raised.suppressed.toList(), "the clean closes attach nothing")
        assertEquals(
            listOf(
                "connect",
                "describe endpoint",
                "attach endpoint",
                "run endpoint child",
                "write ${VIDEO_AVC1.size}b admitted=true",
                "close child",
                "close endpoint",
            ),
            effects,
            "one write, no end, then the child and then the endpoint closed once each",
        )
        assertTrue(pipeline.mayEndTrack(), "no admission is left pending")
        assertEquals(1, source.reads, "no later read")
    }

    @Test
    fun `l1 an exception from a write inside the lifetime settles, then closes the child and the endpoint once, in order`() = runTest {
        raisingWriteInsideTheLifetime(IllegalStateException("the sender failed"))
    }

    @Test
    fun `l2 an error from a write inside the lifetime settles, then closes the child and the endpoint once, in order`() = runTest {
        raisingWriteInsideTheLifetime(AssertionError("an Error inside write"))
    }

    @Test
    fun `l3 a cancelled write inside the lifetime settles, then closes the child and the endpoint once, in order`() = runTest {
        raisingWriteInsideTheLifetime(CancellationException("the write was cancelled"))
    }

    @Test
    fun `f5 a refused endTrack is not a successful end`() = runTest {
        val source = CountingSource(listOf(media(33_333L), emptyEnd()))
        val pipeline = pipeline()
        val sink = ScriptedSink(pipeline, listOf(accept), end = WriteOutcome.WouldBlock)
        assertEquals(TrackDrain.Unfinished("Video: endTrack was refused (WouldBlock)", 1, 0, 0), run(source, pipeline, sink))
        assertEquals(listOf<Call>(written, Call.Ended(mayEndTrack = true)), sink.calls)
    }

    @Test
    fun `c4 the written payload is the declared converted frame`() {
        assertContentEquals(byteArrayOf(0, 0, 0, 7, 0x65, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15), VIDEO_AVC1)
    }

    private companion object {
        val WRITE_TIMEOUT: Duration = 1.seconds
    }
}
