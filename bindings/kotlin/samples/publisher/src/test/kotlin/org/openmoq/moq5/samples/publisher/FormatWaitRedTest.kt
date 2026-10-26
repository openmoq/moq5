package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.MediaType

/**
 * The finite format-readiness barrier in front of the accepted live
 * preparation: both COMPLETE, validated configurations before the first add,
 * one total budget for the pair, and every other exit -- terminal, caller
 * cancellation, a failed source, a commit-grade add outliving the budget --
 * settled by the unchanged coordinator and owner.
 *
 * The formats come from the real validator over SCRIPTED utilities and
 * synthetic literals ([validatedVideo], [validatedAudio]); nothing here
 * certifies an encoder, and the media the rows publish is synthetic. `g` rows
 * measure this file's own oracles and pass now; `p` rows measure the barrier
 * through the real [Rig], the production `livePublication` overload, the
 * same owner and coordinator, and fail at the barrier until it exists.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class FormatWaitRedTest {

    private class HostFormatFailed(message: String) : IllegalStateException(message)

    /** The two validator outputs, each completed by the row when it declares that configuration known. */
    private class Feed {
        val video = CompletableDeferred<CompleteTrackFormat>()
        val audio = CompletableDeferred<CompleteTrackFormat>()

        fun sources() = FormatSources(video, audio)

        fun videoKnown() = apply { assertTrue(video.complete(validatedVideo())) }

        fun audioKnown() = apply { assertTrue(audio.complete(validatedAudio())) }
    }

    /**
     * The DECLARED refusal for an expired budget: it names every track still
     * missing, in video-then-audio order, and the one budget.
     */
    private fun missingFormatsReason(missing: List<MediaKind>, budget: Duration): String {
        require(missing.isNotEmpty())
        val kinds = missing.joinToString(" and ") { it.name.lowercase() }
        val noun = if (missing.size == 1) "format" else "formats"
        return "the $kinds $noun did not arrive within $budget"
    }

    private fun expectMissingFormats(outcome: BroadcastOutcome, missing: List<MediaKind>) {
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(missingFormatsReason(missing, BUDGET), unfinished.reason)
    }

    /**
     * The one total budget: the run has NOT settled one millisecond before
     * [budget] has elapsed since [startedAt], and HAS settled once it has.
     * A budget restarted by a late arrival settles later and fails here.
     */
    private fun TestScope.expectSettledExactlyAt(owner: RunOwner, startedAt: Long, budget: Duration) {
        val deadline = startedAt + budget.inWholeMilliseconds
        val before = deadline - 1 - currentTime
        if (before > 0) advanceTimeBy(before)
        runCurrent()
        assertFalse(owner.reached, "settled before the budget elapsed (at $currentTime ms, deadline $deadline ms)")
        advanceTimeBy(1)
        runCurrent()
        assertTrue(owner.reached, "not settled at the deadline ($deadline ms)")
    }

    /**
     * Prompt settlement, proven WITHOUT a suspending helper: the run has
     * already reached its own boundary when this is called, its recorded
     * result is read synchronously, and the virtual clock still reads [at]
     * after the read. A settlement that needs the scheduler to advance --
     * a delayed failure hidden behind a join -- fails here by name.
     */
    private fun TestScope.promptlyRecorded(owner: RunOwner, at: Long): Result<BroadcastOutcome> {
        assertTrue(owner.reached, "settled promptly: the run had not reached its boundary at $currentTime ms")
        val result = owner.observed()
        assertEquals(at, currentTime, "virtual time unchanged after observing the prompt settlement")
        return result
    }

    /** Nothing the wait must not do: no sender effect, no lane entered, no completion. */
    private fun expectNoEffect(rig: Rig) {
        expectEffects(emptyList(), rig.tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
        assertTrue(rig.completion.calls.isEmpty(), "no completion: ${rig.completion.calls}")
    }

    // ---- g: this file's oracles, executable now ------------------------------------

    /** The missing-kind oracle is exact: the wrong kind, the wrong set, and the wrong budget all fail it. */
    @Test
    fun g1_theMissingFormatOracleRejectsWrongKindsAndBudgets() {
        val video = BroadcastOutcome.Unfinished(missingFormatsReason(listOf(MediaKind.Video), BUDGET), null, null)
        expectMissingFormats(video, listOf(MediaKind.Video))
        assertFailsWith<AssertionError> { expectMissingFormats(video, listOf(MediaKind.Audio)) }
        assertFailsWith<AssertionError> { expectMissingFormats(video, listOf(MediaKind.Video, MediaKind.Audio)) }
        val both = BroadcastOutcome.Unfinished(missingFormatsReason(listOf(MediaKind.Video, MediaKind.Audio), BUDGET), null, null)
        expectMissingFormats(both, listOf(MediaKind.Video, MediaKind.Audio))
        assertFailsWith<AssertionError> { expectMissingFormats(both, listOf(MediaKind.Audio, MediaKind.Video)) }
        val restarted = BroadcastOutcome.Unfinished(missingFormatsReason(listOf(MediaKind.Video), BUDGET * 2), null, null)
        assertFailsWith<AssertionError> { expectMissingFormats(restarted, listOf(MediaKind.Video)) }
        assertFailsWith<AssertionError> { expectMissingFormats(BroadcastOutcome.Unfinished("the session ended", null, null), listOf(MediaKind.Video)) }
    }

    /**
     * The total-budget oracle accepts one deadline -- from a zero start and
     * from a start the scheduler has already advanced past -- and rejects a
     * timer restarted by a mid-way arrival, measured from that same shifted
     * start. Each arm captures its own start; none reuses an absolute zero.
     */
    @Test
    fun g2_theTotalBudgetOracleRejectsARestartedDeadline() = runTest {
        for (arm in 0 until 2) {
            val startedAt = currentTime
            if (arm == 1) assertEquals(BUDGET.inWholeMilliseconds, startedAt, "the second positive arm starts where the first ended")
            val once = withRun({ delay(BUDGET); BroadcastOutcome.Unfinished("once", null, null) }) { owner ->
                expectSettledExactlyAt(owner, startedAt, BUDGET)
                settled(owner)
            }
            assertIs<BroadcastOutcome.Unfinished>(once.getOrThrow())
            assertEquals(startedAt + BUDGET.inWholeMilliseconds, currentTime, "the scheduler reached exactly the intended deadline")
        }
        val startedAt = currentTime
        assertEquals(2 * BUDGET.inWholeMilliseconds, startedAt)
        val arrival = CompletableDeferred<Unit>()
        val restarting =
            escaping {
                withRun({
                    withTimeoutOrNull(BUDGET) { arrival.await() }
                    delay(BUDGET)   /* a budget restarted on arrival */
                    BroadcastOutcome.Unfinished("restarted", null, null)
                }) { owner ->
                    advanceTimeBy(BUDGET.inWholeMilliseconds / 2)
                    arrival.complete(Unit)
                    expectSettledExactlyAt(owner, startedAt, BUDGET)
                }
            }
        val failure = assertNotNull(restarting.failure, "the restarted deadline was rejected")
        assertIs<AssertionError>(failure)
        assertTrue(failure.message!!.startsWith("not settled at the deadline"), failure.message)
        assertEquals(startedAt + BUDGET.inWholeMilliseconds, currentTime, "the oracle drove the scheduler exactly to the intended deadline before rejecting")
    }

    /** The no-early-effect and config-identity oracles are active: an add that happened fails the first; the wrong declared config fails the second; an entered lane fails the first. */
    @Test
    fun g3_theNoEarlyEffectOracleRejectsAnAddAndALaneEntry() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")))
        expectNoEffect(rig)
        assertIs<AddOutcome.Added<HostTrack>>(rig.owner.add(MediaKind.Video, VIDEO_CONFIG))
        assertFailsWith<AssertionError> { expectNoEffect(rig) }
        expectEffects(listOf(Effect.Add(VIDEO_CONFIG)), rig.tracks.all())
        assertFailsWith<AssertionError> { expectEffects(listOf(Effect.Add(AUDIO_CONFIG)), rig.tracks.all()) }
        assertFailsWith<AssertionError> { expectEffects(listOf(Effect.Add(VIDEO_CONFIG.copy(bitrate = 1_500_001uL))), rig.tracks.all()) }
        val clean = Rig()
        clean.videoEntry.signal { }
        assertFailsWith<AssertionError> { expectNoEffect(clean) }
    }

    /**
     * A complete format is typed to its track, configuration and authority
     * alike. Only the validating factories construct one; that no other
     * caller can pair a configuration with an authority is a compile-time
     * boundary, not a runtime row here.
     */
    @Test
    fun g4_aCompleteFormatIsTypedToItsTrack() {
        val video = validatedVideo()
        val audio = validatedAudio()
        assertEquals(MediaType.Video, video.config.mediaType)
        assertEquals(MediaKind.Video, video.kind)
        assertIs<FormatAuthority.Avc>(video.authority)
        assertEquals(MediaType.Audio, audio.config.mediaType)
        assertEquals(MediaKind.Audio, audio.kind)
        assertIs<FormatAuthority.Aac>(audio.authority)
    }

    /** The release/discard inventories are literal: an extra unique release, a wrong generation and a wrong count all fail them. */
    @Test
    fun g5_theReleaseAndDiscardInventoriesAreLiteral() {
        val releases = HostReleases()
        releases.release(0)
        releases.release(1)
        assertEquals(listOf(0, 1), releases.indices())
        releases.release(2)   /* an extra, unique release */
        assertFailsWith<AssertionError> { assertEquals(listOf(0, 1), releases.indices()) }
        val forward = Forward()
        forward.onDiscarded(GENERATION, MediaKind.Video, 1)
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), forward.discards())
        assertFailsWith<AssertionError> { assertEquals(listOf(Discard(GENERATION + 1, MediaKind.Video, 1)), forward.discards()) }
        assertFailsWith<AssertionError> { assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 2)), forward.discards()) }
        assertFailsWith<AssertionError> { assertEquals(listOf(Discard(GENERATION, MediaKind.Audio, 1)), forward.discards()) }
        forward.onDiscarded(GENERATION, MediaKind.Video, 1)   /* the same witness twice is two witnesses */
        assertFailsWith<AssertionError> { assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), forward.discards()) }
    }

    // ---- p: the barrier through the real rig ---------------------------------------

    /** Zero, negative and infinite budgets are refused when the publication is built: before any capture start or add. */
    @Test
    fun p0_aNonPositiveOrInfiniteBudgetIsRefusedBeforeAnyEffect() {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        for (budget in listOf(Duration.ZERO, (-1).seconds, Duration.INFINITE)) {
            assertFailsWith<IllegalArgumentException>("budget $budget") { rig.formatPublication(Feed().sources(), budget) }
        }
        expectNoEffect(rig)
        assertEquals(0, rig.session.stops)
        assertFalse(rig.events.terminal.isCompleted)
        assertFalse(rig.owner.isDecided)
    }

    /** Both formats known before the run: both adds, the declared configs and identities, media through the owned handles, publication. */
    @Test
    fun p1_bothFormatsAlreadyKnownPublishThroughTheOwnedHandles() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val rig = Rig(ScriptedTracks().answers(v).answers(a))
        val feed = Feed().videoKnown().audioKnown()
        publishesAfterBothFormats(rig, feed, v, a) { }
    }

    /** Video known first, audio late but within the budget: nothing until both; then video-then-audio adds and publication. */
    @Test
    fun p2_videoFirstThenAudioAdmitsNothingUntilBoth() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val rig = Rig(ScriptedTracks().answers(v).answers(a))
        val feed = Feed().videoKnown()
        publishesAfterBothFormats(rig, feed, v, a) {
            advanceTimeBy(BUDGET.inWholeMilliseconds / 2)
            runCurrent()
            expectNoEffect(rig)
            feed.audioKnown()
        }
    }

    /** Audio known first, video late but within the budget: the add order is still video then audio. */
    @Test
    fun p3_audioFirstThenVideoStillCreatesVideoThenAudio() = runTest {
        val v = HostTrack("v")
        val a = HostTrack("a")
        val rig = Rig(ScriptedTracks().answers(v).answers(a))
        val feed = Feed().audioKnown()
        publishesAfterBothFormats(rig, feed, v, a) {
            advanceTimeBy(BUDGET.inWholeMilliseconds / 2)
            runCurrent()
            expectNoEffect(rig)
            feed.videoKnown()
        }
    }

    private suspend fun TestScope.publishesAfterBothFormats(rig: Rig, feed: Feed, v: HostTrack, a: HostTrack, beforeBoth: suspend () -> Unit) {
        val videoBytes = VIDEO_ANNEXB.copyOf()
        val audioBytes = byteArrayOf(0x55, 0x66)
        val outcome =
            withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                beforeBoth()
                runCurrent()
                stillLive(owner)
                expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG)), rig.tracks.all())
                assertSame(v, rig.owner.handle(MediaKind.Video))
                assertSame(a, rig.owner.handle(MediaKind.Audio))
                deliver(rig.video, 0, videoBytes, 33_333L)
                runCurrent()
                deliver(rig.audio, 0, audioBytes, 21_333L)
                runCurrent()
                deliverEnd(rig.video, 1)
                runCurrent()
                deliverEnd(rig.audio, 1)
                runCurrent()
                settled(owner)
            }
        assertIs<BroadcastOutcome.Published>(outcome.getOrThrow())
        expectEffects(
            listOf(
                Effect.Add(VIDEO_FORMAT_CONFIG),
                Effect.Add(AUDIO_FORMAT_CONFIG),
                Effect.Write(v, sendObject(VIDEO_AVC1, 33_333L, startsGroup = true), WRITE_TIMEOUT),
                Effect.Write(a, sendObject(audioBytes, 21_333L, startsGroup = false), WRITE_TIMEOUT),
                Effect.End(v, WRITE_TIMEOUT),
                Effect.End(a, WRITE_TIMEOUT),
            ),
            rig.tracks.all(),
        )
        assertEquals(listOf("drain", "complete", "drain"), rig.completion.calls)
        assertTrue(rig.owner.isDecided, "released once")
    }

    /** Neither format: one total deadline, the refusal names both, no effect, the capture settled once. */
    @Test
    fun p4_neitherFormatRefusesBothByNameAtTheOneDeadline() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        refusedAtTheDeadline(rig, Feed(), listOf(MediaKind.Video, MediaKind.Audio)) { }
    }

    /** Audio known at the start, video never: the refusal names video only. */
    @Test
    fun p5_aMissingVideoFormatIsNamedAlone() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        refusedAtTheDeadline(rig, Feed().audioKnown(), listOf(MediaKind.Video)) { }
    }

    /** Video arrives late within the budget, audio never: the deadline is the ORIGINAL one, and the refusal names audio only. */
    @Test
    fun p6_aLateFirstFormatDoesNotRestartTheBudget() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val feed = Feed()
        refusedAtTheDeadline(rig, feed, listOf(MediaKind.Audio)) {
            advanceTimeBy(BUDGET.inWholeMilliseconds / 2)
            runCurrent()
            feed.videoKnown()
            runCurrent()
            expectNoEffect(rig)
        }
    }

    private suspend fun TestScope.refusedAtTheDeadline(rig: Rig, feed: Feed, missing: List<MediaKind>, during: suspend () -> Unit) {
        val startedAt = currentTime
        val outcome =
            withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                during()
                stillLive(owner)
                expectSettledExactlyAt(owner, startedAt, BUDGET)
                settled(owner)
            }
        expectMissingFormats(outcome.getOrThrow(), missing)
        expectNoEffect(rig)
        assertEquals(1, rig.session.stops, "the capture was settled once")
        assertIs<CaptureTermination.Cancelled>(rig.events.terminal.getCompleted())
        assertTrue(rig.owner.isDecided, "the owner was settled")
    }

    /** EOS queued alone during the wait: no drain consumes it before the deadline; absence ends at the deadline; cleanup disposes it with exact releases. */
    @Test
    fun p7_aQueuedEosAloneIsNotConsumedBeforeTheDeadline() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        queuedOutputsWaitForTheDeadline(rig, expectedReleases = listOf(0), expectedDiscards = listOf(Discard(GENERATION, MediaKind.Video, 1))) {
            deliverEnd(rig.video, 0)
        }
    }

    /** A payload then EOS queued during the wait: the same, with both indices released exactly once at queue time. */
    @Test
    fun p8_aQueuedPayloadAndEosAreNotConsumedBeforeTheDeadline() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        queuedOutputsWaitForTheDeadline(rig, expectedReleases = listOf(0, 1), expectedDiscards = listOf(Discard(GENERATION, MediaKind.Video, 2))) {
            deliver(rig.video, 0, byteArrayOf(0x11, 0x22), 33_333L)
            deliverEnd(rig.video, 1)
        }
    }

    /** [expectedReleases] and [expectedDiscards] are DECLARED by the row and held across the whole run; nothing is re-derived from the log after cleanup. */
    private suspend fun TestScope.queuedOutputsWaitForTheDeadline(rig: Rig, expectedReleases: List<Int>, expectedDiscards: List<Discard>, queue: () -> Unit) {
        val feed = Feed()
        val outcome =
            withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                queue()
                runCurrent()
                stillLive(owner)
                assertEquals(expectedReleases, rig.video.releases.indices(), "each queued index was released at queue time, once")
                assertEquals(emptyList(), rig.audio.releases.indices())
                assertEquals(emptyList(), rig.forward.discards(), "nothing discarded before the deadline")
                assertFalse(rig.video.handoff.ended, "the EOS was not dequeued: the end flag is set on dequeue")
                expectNoEffect(rig)
                expectSettledExactlyAt(owner, 0L, BUDGET)
                settled(owner)
            }
        expectMissingFormats(outcome.getOrThrow(), listOf(MediaKind.Video, MediaKind.Audio))
        expectNoEffect(rig)
        assertTrue(rig.video.handoff.ended, "cleanup closed the queue")
        assertEquals(expectedReleases, rig.video.releases.indices(), "the complete release sequence after cleanup is the declared one")
        assertEquals(emptyList(), rig.audio.releases.indices(), "the audio lane released nothing")
        assertEquals(expectedDiscards, rig.forward.discards(), "the cleanup witnessed exactly the declared discards")
    }

    /** The real overflow fault route ends the session during the wait: prompt settlement, no time advanced to the deadline, no track. */
    @Test
    fun p9_anOverflowTerminalDuringTheWaitSettlesPromptly() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val outcome =
            withRun({ rig.run(rig.formatPublication(Feed().sources(), BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                for (i in 0 until CAPACITY) deliver(rig.video, i, byteArrayOf(i.toByte()), 33_333L * (i + 1))
                rig.video.buffers.put(CAPACITY, java.nio.ByteBuffer.wrap(byteArrayOf(0x7f)))
                val refused = rig.video.handoff.onOutput(GENERATION, CodecOutput(CAPACITY, 0, 1, 33_333L * (CAPACITY + 1), CaptureContract.FLAG_KEY_FRAME))
                assertIs<HandoffOutcome.Refused>(refused, "the queue overflowed by the real route")
                runCurrent()
                promptlyRecorded(owner, 0L)
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome.getOrThrow())
        val fault = CaptureFault.Overflow(MediaKind.Video, "the ${MediaKind.Video} queue holds $CAPACITY outputs")   /* the declared fault, whole: the handoff's own wording over its kind and capacity */
        assertEquals(CaptureTermination.Failed(fault), rig.events.terminal.getCompleted())
        assertEquals("the capture session ended before the tracks were prepared: ${CaptureTermination.Failed(fault)}", unfinished.reason)
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided)
        assertEquals((0..CAPACITY).toList(), rig.video.releases.indices(), "every queued index and the refused index released exactly once")
        assertEquals(emptyList(), rig.audio.releases.indices())
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, CAPACITY)), rig.forward.discards(), "the queue's outputs discarded by the failed close, once")
    }

    /** Already-terminal plus already-known formats admits nothing. */
    @Test
    fun p10_anAlreadyEndedSessionWithKnownFormatsAdmitsNothing() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        rig.events.terminal.complete(CaptureTermination.Cancelled("ended before preparation"))
        assertIs<BroadcastOutcome.Unfinished>(rig.run(rig.formatPublication(Feed().videoKnown().audioKnown().sources(), BUDGET)))
        expectNoEffect(rig)
    }

    /** Caller cancellation during the wait: the issued cancellation is the only failure, in both modes; no add; owner settled; nothing left. */
    @Test
    fun p11_callerCancellationDuringTheWaitIsTheOnlyFailure() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(Feed().videoKnown().sources(), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    val cause = owner.cancel("the caller left while waiting for the audio format")
                    runCurrent()
                    expectRoots(expectRecordedCancellation(promptlyRecorded(owner, 0L), cause), emptyList())
                }
            }
        expectNothingLeft(escaped)
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided, "the owner was settled")
        assertEquals(1, rig.session.stops)
    }

    /** A configuration source that fails keeps its Throwable identity: it is the primary, never an absence timeout. */
    @Test
    fun p12_aFailedFormatSourceKeepsItsIdentity() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val failure = HostFormatFailed("the video validator failed")
        val feed = Feed().audioKnown()
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    assertTrue(feed.video.completeExceptionally(failure))
                    runCurrent()
                    expectRoots(expectRecorded(promptlyRecorded(owner, 0L), failure), emptyList())
                }
            }
        expectLeft(escaped, failure)
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided, "the owner was settled")
    }

    /** The converse: the AUDIO source fails while video is ready. */
    @Test
    fun p12b_aFailedAudioSourceKeepsItsIdentity() = runTest {
        val failure = HostFormatFailed("the audio validator failed")
        failedSourceIsThePrimary(Feed().videoKnown(), failure) { feed -> assertTrue(feed.audio.completeExceptionally(failure)) }
    }

    /** A failed video source with audio STILL UNRESOLVED settles promptly: no head-of-line wait, no absence timeout. */
    @Test
    fun p12c_aFailedVideoSourceDoesNotWaitBehindUnresolvedAudio() = runTest {
        val failure = HostFormatFailed("the video validator failed")
        failedSourceIsThePrimary(Feed(), failure) { feed -> assertTrue(feed.video.completeExceptionally(failure)) }
    }

    /** A failed audio source with video STILL UNRESOLVED settles promptly. */
    @Test
    fun p12d_aFailedAudioSourceDoesNotWaitBehindUnresolvedVideo() = runTest {
        val failure = HostFormatFailed("the audio validator failed")
        failedSourceIsThePrimary(Feed(), failure) { feed -> assertTrue(feed.audio.completeExceptionally(failure)) }
    }

    private suspend fun TestScope.failedSourceIsThePrimary(feed: Feed, failure: Throwable, fail: (Feed) -> Unit) {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    fail(feed)
                    runCurrent()
                    expectRoots(expectRecorded(promptlyRecorded(owner, 0L), failure), emptyList())
                }
            }
        expectLeft(escaped, failure)
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided, "the owner was settled")
        assertEquals(1, rig.session.stops)
    }

    /**
     * An independently CANCELLED source (its producer cancelled it; the
     * coordinator did not) with the other source unresolved: prompt
     * settlement; the recorded primary is that cancellation (or its
     * constrained reconstruction), neither an absence timeout nor the
     * coordinator's own cancellation; zero effects; owner settled.
     */
    @Test
    fun p12e_anIndependentlyCancelledSourceIsNeitherATimeoutNorOurCancellation() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val feed = Feed()
        val cancelled = CancellationException("the audio validator was cancelled")
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    feed.audio.cancel(cancelled)
                    runCurrent()
                    val recorded = expectRecordedCancellation(promptlyRecorded(owner, 0L), cancelled)
                    expectRoots(recorded, emptyList())
                    assertFalse(owner.issued(recorded), "not a cancellation this row issued")
                }
            }
        val left = assertNotNull(escaped.failure, "the source's cancellation is an unclaimed fact and leaves the helper")
        if (!isIssuedCancellation(left, cancelled)) throw left   /* anything else that left is what fails the row, as itself */
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided, "the owner was settled")
    }

    /**
     * The instance the runtime CREATED for a cancellation a producer caught:
     * under `-ea` the catch itself receives a reconstruction, whose direct
     * cause of the same class is that instance; under `-da` it is the caught
     * one. Every later reconstruction the runtime makes carries the created
     * instance as its direct cause, so the accepted constrained-copy shape
     * applies against it.
     */
    private fun createdInstanceOf(caught: CancellationException): CancellationException {
        val cause = caught.cause
        return if (cause is CancellationException && cause::class == caught::class) cause else caught
    }

    /** The origin normalization is one hop of the accepted shape: a same-class direct cause, and nothing else. */
    @Test
    fun g6_theCreatedInstanceIsOneSameClassHop() {
        val created = CancellationException("created")
        val caught = CancellationException("created").also { it.initCause(created) }
        assertSame(created, createdInstanceOf(caught))
        assertSame(created, createdInstanceOf(created))
        val wrapped = CancellationException("wrapped").also { it.initCause(IllegalStateException("other", created)) }
        assertSame(wrapped, createdInstanceOf(wrapped), "another class between is not a reconstruction")
        val deeper = CancellationException("deeper").also { it.initCause(caught) }
        assertSame(caught, createdInstanceOf(deeper), "exactly one hop")
    }

    /**
     * A NESTED timeout inside a source's producer is that producer's own
     * failure: it keeps its identity and is never relabelled as the
     * barrier's absence refusal. A catch-all timeout-to-Refused cannot pass.
     */
    @Test
    fun p12f_aNestedTimeoutInsideASourceIsNotTheAbsenceRefusal() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val feed = Feed().videoKnown()
        var nested: Throwable? = null
        var recorded: Throwable? = null
        val producer =
            launch {
                try {
                    withTimeout(1) { awaitCancellation() }
                } catch (timeout: TimeoutCancellationException) {
                    nested = timeout
                    feed.audio.completeExceptionally(timeout)
                }
            }
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    advanceTimeBy(1)
                    runCurrent()
                    producer.join()
                    val timeout = assertIs<TimeoutCancellationException>(nested, "the producer's own timeout fired")
                    val created = createdInstanceOf(timeout)
                    recorded = expectRecordedCancellation(promptlyRecorded(owner, 1L), created)   /* at the producer's 1 ms, on the run's OWN frame: the created instance or its constrained reconstruction */
                    expectRoots(recorded!!, emptyList())
                }
            }
        val left = assertNotNull(escaped.failure, "the producer's timeout is an unclaimed fact and leaves the helper")
        val created = (nested as? TimeoutCancellationException)?.let { createdInstanceOf(it) } ?: throw left
        if (!isIssuedCancellation(left, created)) throw left   /* anything else that left is what fails the row, as itself */
        assertNotNull(recorded)
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided)
    }

    /** A valid AUDIO format resolved in the VIDEO slot is a contract failure before either add. */
    @Test
    fun p15_anAudioFormatInTheVideoSlotIsRefusedBeforeAnyAdd() = runTest {
        slotMismatchIsRefused(video = validatedAudio(), audio = validatedAudio(), expectedMessage = "the video source resolved an audio format")
    }

    /** A valid VIDEO format resolved in the AUDIO slot is a contract failure before either add. */
    @Test
    fun p16_aVideoFormatInTheAudioSlotIsRefusedBeforeAnyAdd() = runTest {
        slotMismatchIsRefused(video = validatedVideo(), audio = validatedVideo(), expectedMessage = "the audio source resolved a video format")
    }

    /** Both valid formats swapped between the slots: refused, naming the video slot first. */
    @Test
    fun p17_aSwappedValidPairIsRefusedBeforeAnyAdd() = runTest {
        slotMismatchIsRefused(video = validatedAudio(), audio = validatedVideo(), expectedMessage = "the video source resolved an audio format")
    }

    private suspend fun TestScope.slotMismatchIsRefused(video: CompleteTrackFormat, audio: CompleteTrackFormat, expectedMessage: String) {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val feed = Feed()
        assertTrue(feed.video.complete(video))
        assertTrue(feed.audio.complete(audio))
        var recorded: IllegalArgumentException? = null
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                    runCurrent()
                    expectNoEffect(rig)   /* refused BEFORE either add: a wrong-config add fails here, by inventory */
                    val primary = assertNotNull(promptlyRecorded(owner, 0L).exceptionOrNull(), "the contract failure is the run's own recorded failure")
                    val contract = primary as? IllegalArgumentException ?: throw primary   /* anything else recorded is what fails the row, as itself */
                    assertEquals(expectedMessage, contract.message)
                    expectRoots(contract, emptyList())
                    recorded = contract
                }
            }
        val left = assertNotNull(escaped.failure, "the contract failure leaves the helper")
        val primary = recorded ?: throw left   /* the boundary assertions did not run: what left is what fails the row, as itself */
        if (!isSameOrRecoveredCopyOf(left, primary)) throw left   /* the recorded instance, or the helper crossing's constrained copy of it; anything else fails as itself */
        assertEquals(expectedMessage, left.message)
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided, "the owner was settled")
        assertEquals(1, rig.session.stops)
    }

    /** The sources are BORROWED: an expired budget leaves the pending producers uncancelled, and a format they resolve afterwards has no sender effect. */
    @Test
    fun p18_anExpiredBudgetDoesNotCancelPendingSourcesAndALateFormatHasNoEffect() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val feed = Feed()
        val outcome =
            withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                expectSettledExactlyAt(owner, 0L, BUDGET)
                owner.observed()
            }
        expectMissingFormats(outcome.getOrThrow(), listOf(MediaKind.Video, MediaKind.Audio))
        assertTrue(feed.video.isActive && feed.audio.isActive, "the borrowed sources were not cancelled by the expired budget")
        feed.videoKnown().audioKnown()
        runCurrent()
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided)
    }

    /** Caller cancellation during the wait leaves the borrowed sources uncancelled; a format resolved afterwards has no sender effect. */
    @Test
    fun p19_callerCancellationDoesNotCancelPendingSourcesAndALateFormatHasNoEffect() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val feed = Feed()
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(feed.sources(), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    val cause = owner.cancel("the caller left while both formats were pending")
                    runCurrent()
                    expectRoots(expectRecordedCancellation(promptlyRecorded(owner, 0L), cause), emptyList())
                }
            }
        expectNothingLeft(escaped)
        assertTrue(feed.video.isActive && feed.audio.isActive, "the borrowed sources were not cancelled by the caller's cancellation")
        feed.videoKnown().audioKnown()
        runCurrent()
        expectNoEffect(rig)
        assertTrue(rig.owner.isDecided)
    }

    /** The pair resolves, then a held commit-grade add outlives the budget: the expired budget does not refuse, the handle is owned, publication proceeds. */
    @Test
    fun p13_aHeldAddOutlivingTheBudgetStaysOwned() = runTest {
        val hold = CompletableDeferred<Unit>()
        val v = HostTrack("v")
        val a = HostTrack("a")
        val tracks = ScriptedTracks().answers(v, hold).answers(a)
        val entered = tracks.entering()
        val rig = Rig(tracks)
        val outcome =
            withRun({ rig.run(rig.formatPublication(Feed().videoKnown().audioKnown().sources(), BUDGET)) }) { owner ->
                withHolds(listOf(hold)) {
                    runCurrent()
                    stillLive(owner)
                    withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the video add was never entered")
                    advanceTimeBy(BUDGET.inWholeMilliseconds * 2)
                    runCurrent()
                    stillLive(owner)
                    assertFalse(owner.reached, "the expired format budget did not settle a run whose add is in flight")
                    hold.complete(Unit)
                    runCurrent()
                    stillLive(owner)
                    assertSame(v, rig.owner.handle(MediaKind.Video))
                    assertSame(a, rig.owner.handle(MediaKind.Audio))
                    deliverEnd(rig.video, 0)
                    runCurrent()
                    deliverEnd(rig.audio, 0)
                    runCurrent()
                    settled(owner)
                }
            }
        assertIs<BroadcastOutcome.Published>(outcome.getOrThrow())
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG), Effect.End(v, WRITE_TIMEOUT), Effect.End(a, WRITE_TIMEOUT)), tracks.all())
    }

    /** A held add outlives the budget while the caller cancels: the returned handle is owned and retired once; the cancellation is the primary. */
    @Test
    fun p14_aHeldAddOutlivingTheBudgetUnderCancellationIsRetiredOnce() = runTest {
        val hold = CompletableDeferred<Unit>()
        val v = HostTrack("v")
        val tracks = ScriptedTracks().answers(v, hold).answers(HostTrack("a"))
        val entered = tracks.entering()
        val rig = Rig(tracks)
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(Feed().videoKnown().audioKnown().sources(), BUDGET)) }) { owner ->
                    withHolds(listOf(hold)) {
                        runCurrent()
                        stillLive(owner)
                        withTimeoutOrNull(BOUNDED) { entered.await() } ?: throw AssertionError("the video add was never entered")
                        advanceTimeBy(BUDGET.inWholeMilliseconds * 2)
                        runCurrent()
                        stillLive(owner)
                        val cause = owner.cancel("the user left")
                        runCurrent()
                        assertFalse(owner.reached, "not decided before the held add returns")
                        hold.complete(Unit)
                        runCurrent()
                        expectRoots(expectRecordedCancellation(settled(owner), cause), emptyList())
                    }
                }
            }
        expectNothingLeft(escaped)
        expectEffects(listOf(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Remove(v)), tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
    }

    private companion object {
        val BUDGET = 2.seconds
    }
}
