package org.openmoq.moq5.samples.publisher

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.SampleEntry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The format holders, end to end: typed encoder observations through the
 * real validators into the real slots, admitted by the real session, awaited
 * by the real format-wait preparation and settled by the real coordinator.
 * Only the codec utilities are scripted -- counting, and where a row needs
 * it holding or raising -- over the fixture's declared answers.
 *
 * A held validation runs on an owned worker; every row releases its holds
 * and joins its workers on every path, with bounded joins.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class FormatHolderTest {

    /** The AVC utility over the fixture's answers: counts each record build, and can hold or raise inside it. */
    private class VideoUtility : AvcRecordUtility {
        val builds = AtomicInteger()
        val entered = CountDownLatch(1)
        private val release = CountDownLatch(1)

        @Volatile var holdNext = false

        @Volatile var raise: Throwable? = null

        fun rescue() = release.countDown()

        override fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): UtilityOutcome<MediaBytes> {
            builds.incrementAndGet()
            if (holdNext) {
                holdNext = false
                entered.countDown()
                check(release.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held validation was never released" }
            }
            raise?.let { throw it }
            return ScriptedVideoUtility.buildInitData(source, bytes, nalLengthSize)
        }

        override fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes): UtilityOutcome<String> =
            ScriptedVideoUtility.formatCodecString(format, sampleEntry, decoderConfig)
    }

    /** The AAC string utility over the fixture's answer, counted. */
    private class AudioUtility : AacStringUtility {
        val calls = AtomicInteger()

        override fun formatCodecString(audioSpecificConfig: MediaBytes): UtilityOutcome<String> {
            calls.incrementAndGet()
            return ScriptedAudioUtility.formatCodecString(audioSpecificConfig)
        }
    }

    /** Runs [body] on its own thread, keeping what it raised or returned; joined with a bound. */
    private class Worker<T>(name: String, body: () -> T) {
        private val result = AtomicReference<Result<T>?>()
        private val thread = Thread({ result.set(runCatching(body)) }, name)

        fun start() = thread.start()

        fun join() {
            if (thread.state != Thread.State.NEW) thread.join(BOUND_SECONDS * 1_000L)
        }

        val joined: Boolean get() = thread.state != Thread.State.NEW && !thread.isAlive

        val outcome: Result<T>? get() = result.get()
    }

    private class Setup(val rig: Rig, val video: VideoUtility, val audio: AudioUtility, val videoSlot: TrackFormatSlot, val audioSlot: TrackFormatSlot) {
        val videoHolder = VideoFormatHolder(rig.gate, videoSlot, VIDEO_CONFIG.name, FIXTURE_BOUNDS, video)
        val audioHolder = AudioFormatHolder(rig.gate, audioSlot, AUDIO_CONFIG.name, FIXTURE_BOUNDS, audio)
        val sources = FormatSources(videoHolder.format, audioHolder.format)
    }

    private fun setup(): Setup = Setup(Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))), VideoUtility(), AudioUtility(), TrackFormatSlot(MediaKind.Video), TrackFormatSlot(MediaKind.Audio))

    private fun causeChain(raised: Throwable?, expected: Throwable) = generateSequence(raised) { it.cause }.any { it === expected }

    private fun Throwable.suppressedCount(of: Throwable) = suppressed.count { it === of }

    private fun adds(rig: Rig) = rig.tracks.all().filterIsInstance<Effect.Add>().map { it.config }

    /** The owner over the holders' sources; [body] drives it and releases its own holds before this settles the owner. */
    private suspend fun TestScope.owned(s: Setup, body: suspend (RunOwner) -> Unit): Result<BroadcastOutcome>? {
        var outcome: Result<BroadcastOutcome>? = null
        runCatching {
            withRun({ s.rig.run(s.rig.formatPublication(s.sources, BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                body(owner)
                runCurrent()
                outcome = settled(owner)
            }
        }
        return outcome
    }

    // -- resolution ------------------------------------------------------------------------------

    @Test
    fun `h1 each kind validates on its own, nothing is added before both resolve, and an equal repeat keeps the first format`() = runTest {
        for (audioFirst in listOf(true, false)) {
            val s = setup()
            var addsAfterFirst: List<Any>? = null
            var first: FormatOutcome? = null
            var second: FormatOutcome? = null
            var repeat: FormatOutcome? = null
            val outcome =
                owned(s) {
                    first = if (audioFirst) s.audioHolder.onOutputFormat(audioObservation()) else s.videoHolder.onOutputFormat(videoObservation())
                    runCurrent()
                    addsAfterFirst = adds(s.rig)
                    second = if (audioFirst) s.videoHolder.onOutputFormat(videoObservation()) else s.audioHolder.onOutputFormat(audioObservation())
                    runCurrent()
                    repeat = s.videoHolder.onOutputFormat(videoObservation())
                    deliverEnd(s.rig.video, 0)
                    deliverEnd(s.rig.audio, 0)
                }
            val label = if (audioFirst) "audio first" else "video first"
            assertIs<FormatOutcome.Resolved>(first, label)
            assertEquals(emptyList(), addsAfterFirst, "$label: nothing is added before both formats resolve")
            val videoFormat = if (audioFirst) assertIs<FormatOutcome.Resolved>(second).format else assertIs<FormatOutcome.Resolved>(first).format
            assertSame(FormatOutcome.Equal, repeat, "$label: an equal repeat does nothing")
            assertSame(videoFormat, s.videoSlot.format.getCompleted(), "$label: the first format is the one held")
            assertEquals(listOf(VIDEO_FORMAT_CONFIG, AUDIO_FORMAT_CONFIG), adds(s.rig), "$label: the exact configurations, video then audio")
            assertIs<BroadcastOutcome.Published>(checkNotNull(outcome).getOrThrow(), label)
            assertEquals(2, s.video.builds.get(), "$label: the first video and the equal repeat were each validated")
            assertEquals(1, s.audio.calls.get())
            assertEquals(FormatInventory(emptySet(), emptyList(), CaptureTermination.Completed), s.rig.gate.formatInventory())
        }
    }

    @Test
    fun `h2 a first refusal fails the format wait before any add, without calling the utility`() = runTest {
        val s = setup()
        var refused: FormatOutcome? = null
        val outcome = owned(s) { refused = s.videoHolder.onOutputFormat(videoObservation(width = null)) }
        val failure = assertIs<FormatOutcome.Refused>(refused).failure
        assertEquals(TrackFormatRefusal.MissingFact("width"), failure.refusal)
        assertEquals(0, s.video.builds.get(), "the declared facts are checked before any utility call")
        assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), failure), "the refusal is the wait's failure: $outcome")
        assertEquals(emptyList(), adds(s.rig), "no track was added")
        assertEquals(emptySet(), s.rig.gate.formatInventory().active)
    }

    @Test
    fun `h3 a later changed or refused format is the track's format fault, and the first format is kept`() = runTest {
        for (videoChanges in listOf(true, false)) {
            val s = setup()
            var later: FormatOutcome? = null
            val outcome =
                owned(s) {
                    s.videoHolder.onOutputFormat(videoObservation())
                    s.audioHolder.onOutputFormat(audioObservation())
                    runCurrent()
                    later = if (videoChanges) s.videoHolder.onOutputFormat(videoObservation(width = 1280)) else s.audioHolder.onOutputFormat(audioObservation(sampleRate = null))
                }
            val changed = assertIs<FormatOutcome.Changed>(later)
            val expected =
                if (videoChanges) {
                    CaptureFault.Format(MediaKind.Video, "the video output format changed after it was validated")
                } else {
                    CaptureFault.Format(MediaKind.Audio, "the output format declared no sample rate")
                }
            assertEquals(expected, changed.fault)
            assertEquals(CaptureTermination.Failed(expected), s.rig.session.termination, "the fault ended the session")
            val slot = if (videoChanges) s.videoSlot else s.audioSlot
            assertSame(changed.held, slot.format.getCompleted(), "the held format stays")
            assertEquals(listOf(VIDEO_FORMAT_CONFIG, AUDIO_FORMAT_CONFIG), adds(s.rig), "no track was re-added with the new format")
            assertIs<BroadcastOutcome.Unfinished>(checkNotNull(outcome).getOrThrow())
            assertEquals(listOf("add:video", "add:audio", "remove:v", "remove:a"), s.rig.tracks.labels())
            assertEquals(if (videoChanges) 2 else 1, s.video.builds.get())
            assertEquals(1, s.audio.calls.get(), "a refused later audio format never reached the utility")
            assertEquals(emptyList(), s.rig.session.retirement().causes, "a format fault is not an unexpected cause")
        }
    }

    // -- ownership across delivery and other kinds ----------------------------------------------

    @Test
    fun `h4 an observation re-entering from its own slot delivery overlaps -- no second reservation, no validation -- and both locks are free`() = runTest {
        val s = setup()
        var inner: FormatOutcome? = null
        var activeInside: Set<MediaKind>? = null
        var sessionLockFree = false
        var slotLockFree = false
        s.videoHolder.format.invokeOnCompletion {
            activeInside = s.rig.gate.formatInventory().active
            val readInventory = AtomicReference<FormatInventory?>()
            val offered = AtomicReference<SlotOffer?>()
            val probe =
                Thread({
                    readInventory.set(s.rig.gate.formatInventory())
                    offered.set(s.videoSlot.offer(TrackFormatVerdict.Complete(validatedVideo())))
                }, "lock-probe").apply { start() }
            probe.join(BOUND_SECONDS * 1_000L)
            sessionLockFree = !probe.isAlive && readInventory.get() != null
            slotLockFree = offered.get() != null
            inner = s.videoHolder.onOutputFormat(videoObservation())
        }
        var outer: FormatOutcome? = null
        val outcome = owned(s) { outer = s.videoHolder.onOutputFormat(videoObservation()) }
        assertIs<FormatOutcome.Resolved>(outer, "the outer observation resolved the slot")
        assertSame(FormatOutcome.Overlapping, inner, "the re-entry overlapped")
        assertEquals(setOf(MediaKind.Video), activeInside, "the outer reservation was still held during delivery")
        assertTrue(sessionLockFree, "another thread read the session while the handler ran")
        assertTrue(slotLockFree, "and offered to the slot")
        assertEquals(1, s.video.builds.get(), "the overlap validated nothing")
        val overlap = CaptureFault.Format(MediaKind.Video, "overlapping output-format observations")
        assertEquals(CaptureTermination.Failed(overlap), s.rig.session.termination, "the overlap is the track's format fault")
        assertIs<BroadcastOutcome.Unfinished>(checkNotNull(outcome).getOrThrow())
        assertEquals(emptyList(), adds(s.rig))
        assertEquals(emptySet(), s.rig.gate.formatInventory().active, "one reservation, released once")
    }

    @Test
    fun `h5 the other kind validates and resolves while one validation is held`() = runTest {
        val s = setup()
        s.video.holdNext = true
        val worker = Worker("video-format") { s.videoHolder.onOutputFormat(videoObservation()) }
        var activeWhileHeld: Set<MediaKind>? = null
        var audio: FormatOutcome? = null
        var addsWhileHeld: List<Any>? = null
        val outcome =
            owned(s) {
                try {
                    worker.start()
                    assertTrue(s.video.entered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the video validation is held")
                    audio = s.audioHolder.onOutputFormat(audioObservation())
                    runCurrent()
                    activeWhileHeld = s.rig.gate.formatInventory().active
                    addsWhileHeld = adds(s.rig)
                } finally {
                    s.video.rescue()
                    worker.join()
                }
                runCurrent()
                deliverEnd(s.rig.video, 0)
                deliverEnd(s.rig.audio, 0)
            }
        assertIs<FormatOutcome.Resolved>(audio, "audio resolved while video was held")
        assertEquals(setOf(MediaKind.Video), activeWhileHeld, "only the held video callback was admitted")
        assertEquals(emptyList(), addsWhileHeld, "nothing added while video was unresolved")
        assertIs<FormatOutcome.Resolved>(checkNotNull(worker.outcome).getOrThrow())
        assertIs<BroadcastOutcome.Published>(checkNotNull(outcome).getOrThrow())
        assertEquals(listOf(VIDEO_FORMAT_CONFIG, AUDIO_FORMAT_CONFIG), adds(s.rig))
    }

    // -- a held validation across the ending ----------------------------------------------------

    /**
     * The video validation is held on a worker while the session fails; the
     * owner cannot settle, nothing new is admitted, and whatever the held
     * validation then raises is the owner's failure, as itself.
     */
    @Test
    fun `h6 a validation held across a failure keeps the owner waiting, and what it then raises survives as itself`() = runTest {
        val variants = listOf<Throwable?>(IllegalStateException("the utility failed"), AssertionError("the utility raised an Error"), kotlinx.coroutines.CancellationException("cancelled inside the utility"), null)
        for (raised in variants) {
            val s = setup()
            s.video.holdNext = true
            s.video.raise = raised
            val worker = Worker("video-format") { s.videoHolder.onOutputFormat(videoObservation()) }
            val fault = CaptureFault.Codec(MediaKind.Audio, "the audio encoder failed")
            var ownerDoneWhileHeld = true
            var laterWhileHeld: FormatOutcome? = null
            val outcome =
                owned(s) { owner ->
                    try {
                        worker.start()
                        assertTrue(s.video.entered.await(BOUND_SECONDS, TimeUnit.SECONDS))
                        s.rig.forward.onFault(GENERATION, fault)
                        runCurrent()
                        ownerDoneWhileHeld = owner.job.isCompleted
                        laterWhileHeld = s.audioHolder.onOutputFormat(audioObservation())
                    } finally {
                        s.video.rescue()
                        worker.join()
                    }
                }
            val label = raised?.let { it::class.simpleName } ?: "clean"
            assertFalse(ownerDoneWhileHeld, "$label: the owner had not settled while the validation was admitted")
            assertSame(FormatOutcome.Ended, laterWhileHeld, "$label: nothing new is admitted after the claim")
            assertEquals(0, s.audio.calls.get(), "$label: and it validated nothing")
            assertTrue(worker.joined)
            val receipt = s.rig.session.retirement()
            assertEquals(CaptureTermination.Failed(fault), receipt.termination)
            if (raised == null) {
                assertIs<FormatOutcome.Resolved>(checkNotNull(worker.outcome).getOrThrow(), "a late resolution is harmless")
                assertEquals(emptyList(), receipt.causes)
                assertIs<BroadcastOutcome.Unfinished>(checkNotNull(outcome).getOrThrow())
            } else {
                assertSame(raised, checkNotNull(worker.outcome).exceptionOrNull(), "$label: the holder rethrows the original to its caller")
                assertEquals(listOf(raised), receipt.causes, "$label: recorded once, as itself")
                assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), raised), "$label: the owner's failure, never an ordinary Unfinished: $outcome")
                assertTrue(s.videoSlot.format.isCompleted && causeChain(s.videoSlot.format.getCompletionExceptionOrNull(), raised), "$label: the unsettled slot failed with the same cause")
            }
            assertEquals(emptySet(), s.rig.gate.formatInventory().active, "$label: released once")
            assertEquals(emptyList(), adds(s.rig))
        }
    }

    /**
     * A later video observation is held while the tracks end: Completed wins,
     * nothing is published, and a cause raised after the slot had resolved
     * still becomes the broadcast's failure.
     */
    @Test
    fun `h6b a cause raised after the slot resolved still survives Completed`() = runTest {
        for (raised in listOf<Throwable?>(IllegalStateException("the utility failed on a repeat"), null)) {
            val s = setup()
            val worker = Worker("video-format") { s.videoHolder.onOutputFormat(videoObservation()) }
            var publishedWhileHeld = true
            var completionWhileHeld: List<String> = listOf("?")
            val outcome =
                owned(s) { owner ->
                    try {
                        s.videoHolder.onOutputFormat(videoObservation())
                        s.audioHolder.onOutputFormat(audioObservation())
                        runCurrent()
                        s.video.holdNext = true
                        s.video.raise = raised
                        worker.start()
                        assertTrue(s.video.entered.await(BOUND_SECONDS, TimeUnit.SECONDS))
                        deliverEnd(s.rig.video, 0)
                        deliverEnd(s.rig.audio, 0)
                        runCurrent()
                        publishedWhileHeld = owner.job.isCompleted
                        completionWhileHeld = s.rig.completion.calls.toList()
                        assertEquals(CaptureTermination.Completed, s.rig.session.termination)
                    } finally {
                        s.video.rescue()
                        worker.join()
                    }
                }
            val label = if (raised == null) "clean" else "late cause"
            assertFalse(publishedWhileHeld, "$label: nothing was published while the repeat was admitted")
            assertEquals(emptyList(), completionWhileHeld, "$label: and the sender was told nothing")
            if (raised == null) {
                assertSame(FormatOutcome.Equal, checkNotNull(worker.outcome).getOrThrow())
                assertIs<BroadcastOutcome.Published>(checkNotNull(outcome).getOrThrow())
                assertEquals(listOf("drain", "complete", "drain"), s.rig.completion.calls)
            } else {
                assertSame(raised, checkNotNull(worker.outcome).exceptionOrNull())
                assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), raised), "the late cause is the broadcast's failure: $outcome")
                assertEquals(listOf<Throwable>(raised), s.rig.session.retirement().causes)
                assertEquals(emptyList(), s.rig.completion.calls, "never completed")
                assertIs<CompleteTrackFormat>(s.videoSlot.format.getCompleted(), "the resolved slot is untouched")
            }
        }
    }

    @Test
    fun `h6c the owner's own cancellation stays primary over a held validation, and carries its Error once`() = runTest {
        val s = setup()
        val raised = AssertionError("the utility raised an Error after cancellation")
        s.video.holdNext = true
        s.video.raise = raised
        val worker = Worker("video-format") { s.videoHolder.onOutputFormat(videoObservation()) }
        var ownerDoneWhileHeld = true
        var why: Throwable? = null
        var run: RunOwner? = null
        val outcome =
            owned(s) { owner ->
                try {
                    worker.start()
                    assertTrue(s.video.entered.await(BOUND_SECONDS, TimeUnit.SECONDS))
                    run = owner
                    why = owner.cancel("the row stops the broadcast")
                    runCurrent()
                    ownerDoneWhileHeld = owner.job.isCompleted
                } finally {
                    s.video.rescue()
                    worker.join()
                }
            }
        assertFalse(ownerDoneWhileHeld)
        val failure = checkNotNull(outcome).exceptionOrNull()
        assertTrue(checkNotNull(run).issued(checkNotNull(failure)) && causeChain(failure, checkNotNull(why)), "the owner's own cancellation leaves: $failure")
        assertEquals(1, generateSequence(failure) { it.cause }.sumOf { it.suppressedCount(raised) }, "carrying the held validation's Error once")
        assertSame(raised, checkNotNull(worker.outcome).exceptionOrNull())
        assertEquals(listOf<Throwable>(raised), s.rig.session.retirement().causes)
    }

    // -- failures on the way out ----------------------------------------------------------------

    @Test
    fun `h7 a cleanup failure the format fault's claimant raises reaches the caller, and the reservation is released once`() = runTest {
        for (cleanup in listOf<Throwable>(IllegalStateException("the video close failed after the real handoff closed"), kotlinx.coroutines.CancellationException("the video close was cancelled after the real handoff closed"))) {
        val s = setup()
        s.rig.videoEntry.beforeClose = {
            s.rig.video.handoff.close(CloseMode.Failed)
            throw cleanup
        }
        var raised: Throwable? = null
        val outcome =
            owned(s) {
                s.videoHolder.onOutputFormat(videoObservation())
                raised = runCatching { s.videoHolder.onOutputFormat(videoObservation(width = 1280)) }.exceptionOrNull()
            }
        assertSame(cleanup, raised, "the claimant's cleanup failure reached the caller")
        assertEquals(emptySet(), s.rig.gate.formatInventory().active, "released exactly once")
        val receipt = s.rig.session.retirement()
        assertEquals(listOf<Throwable>(cleanup), receipt.cleanup, "the receipt owns it")
        assertEquals(emptyList(), receipt.causes, "and it is not an unexpected cause")
        assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), cleanup), "the owner carries it: $outcome")
        assertEquals(emptyList(), s.rig.completion.calls)
        }
    }

    @Test
    fun `h8 a handler failure during the slot's own delivery is an unexpected cause, owned once and rethrown`() = runTest {
        val s = setup()
        val handlerFailure = IllegalStateException("a handler on the video format failed")
        s.videoHolder.format.invokeOnCompletion { throw handlerFailure }
        var raised: Throwable? = null
        val outcome = owned(s) { raised = runCatching { s.videoHolder.onOutputFormat(videoObservation()) }.exceptionOrNull() }
        assertTrue(causeChain(raised, handlerFailure), "the delivery's failure reached the caller: $raised")
        val cause = checkNotNull(raised)
        assertEquals(listOf(cause), s.rig.session.retirement().causes, "recorded once, as the caller saw it")
        assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, cause)), s.rig.session.termination)
        assertIs<CompleteTrackFormat>(s.videoSlot.format.getCompleted(), "the slot had already resolved; it is not failed afterwards")
        assertEquals(emptySet(), s.rig.gate.formatInventory().active)
        assertTrue(causeChain(checkNotNull(outcome).exceptionOrNull(), cause), "the owner's failure: $outcome")
        assertNull(s.rig.session.retirement().cleanup.firstOrNull())
    }

    /**
     * An unexpected validation failure claims the ending through its release;
     * the claimant's close then raises a DIFFERENT failure -- an Exception, an
     * Error or a cancellation -- after the real close. The caller still gets
     * the original cause, as itself; the release happened once; the receipt
     * owns both, each once; and the coordinator's failure is the original,
     * carrying the cleanup failure once.
     */
    @Test
    fun `h9 a cleanup failure raised while releasing an unexpected cause never replaces that cause at the caller`() = runTest {
        val secondaries = listOf<Throwable>(kotlinx.coroutines.CancellationException("the video close was cancelled"), IllegalStateException("the video close failed"), AssertionError("the video close raised an Error"))
        for (secondary in secondaries) {
            val s = setup()
            val original = AssertionError("the utility raised while validating")
            s.video.raise = original
            s.rig.videoEntry.beforeClose = {
                s.rig.video.handoff.close(CloseMode.Failed)
                throw secondary
            }
            var raised: Throwable? = null
            val outcome = owned(s) { raised = runCatching { s.videoHolder.onOutputFormat(videoObservation()) }.exceptionOrNull() }
            val label = secondary::class.simpleName
            assertSame(original, raised, "$label: the caller gets the original cause, not the cleanup failure")
            assertEquals(emptySet(), s.rig.gate.formatInventory().active, "$label: the reservation was released")
            val receipt = s.rig.session.retirement()
            assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, original)), receipt.termination)
            assertEquals(listOf<Throwable>(original), receipt.causes, "$label: the cause, once")
            assertEquals(listOf(secondary), receipt.cleanup, "$label: the cleanup failure, once, owned by the receipt")
            assertTrue(causeChain(runCatching { s.videoSlot.format.getCompleted() }.exceptionOrNull(), original), "$label: the unsettled slot failed with the original")
            val failure = checkNotNull(outcome).exceptionOrNull()
            assertTrue(causeChain(failure, original), "$label: the owner's failure is the original: $outcome")
            assertEquals(1, generateSequence(failure) { it.cause }.sumOf { it.suppressedCount(secondary) }, "$label: carrying the cleanup failure once")
            assertEquals(emptyList(), s.rig.completion.calls, "$label: no sender completion")
            assertEquals(emptyList(), adds(s.rig))
        }
    }

    private companion object {
        val BUDGET = 5.seconds
        const val BOUND_SECONDS = 10L
    }
}
