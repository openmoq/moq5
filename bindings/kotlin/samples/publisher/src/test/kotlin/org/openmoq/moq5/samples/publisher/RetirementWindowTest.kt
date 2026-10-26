package org.openmoq.moq5.samples.publisher

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The retirement window of the real session under the real owner: the
 * session CLAIMS its ending in one step, closes its handoffs in the next,
 * and delivers its terminal only after that. Held at the first close, the
 * claim is visible while the handoffs are still open and the terminal is not
 * yet delivered -- the interval a late format resolution would land in.
 *
 * The fault arrives on an owned worker thread through the session's own
 * fault route, as a format callback's would; the close gate is the fixture's
 * forwarding handoff, and the close it gates is the real one. The worker is
 * joined, with a bounded wait, before anything is asserted afterwards.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class RetirementWindowTest {

    @Test
    fun `o1 a claimed ending precedes the first handoff close and the terminal delivery`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val videoSlot = CompletableDeferred<CompleteTrackFormat>()
        val audioSlot = CompletableDeferred<CompleteTrackFormat>()
        videoSlot.complete(validatedVideo())
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        rig.videoEntry.beforeClose = {
            entered.countDown()
            check(release.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the gated close was never released" }
        }
        val workerFailure = AtomicReference<Throwable?>()
        val fault = CaptureFault.Codec(MediaKind.Video, "a fault from outside the handoff")
        val worker = Thread({ runCatching { rig.forward.onFault(GENERATION, fault) }.exceptionOrNull()?.let { workerFailure.set(it) } }, "retirement-window-worker")
        withRun({ rig.run(rig.formatPublication(FormatSources(videoSlot, audioSlot), BUDGET)) }) { owner ->
            runCurrent()
            stillLive(owner)
            try {
                worker.start()
                assertTrue(entered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the first close was reached")
                assertEquals(CaptureTermination.Failed(fault), rig.session.termination, "the ending is claimed")
                assertFalse(rig.video.handoff.ended, "the video handoff is not yet closed")
                assertFalse(rig.audio.handoff.ended, "nor the audio handoff")
                assertFalse(rig.events.terminal.isCompleted, "and the terminal is not yet delivered")
            } finally {
                release.countDown()
                if (worker.state != Thread.State.NEW) worker.join(BOUND_SECONDS * 1_000L)
            }
            assertFalse(worker.isAlive, "the worker was joined")
            assertNull(workerFailure.get(), "the close raised nothing")
            assertTrue(rig.video.handoff.ended && rig.audio.handoff.ended, "both handoffs closed")
            assertTrue(rig.events.terminal.isCompleted, "the terminal is delivered after them")
            runCurrent()
            val outcome = assertIs<BroadcastOutcome.Unfinished>(settled(owner).getOrThrow())
            assertTrue(outcome.reason.isNotEmpty())
        }
        expectEffects(emptyList(), rig.tracks.all())
        assertTrue(rig.owner.isDecided)
    }

    private companion object {
        val BUDGET = 5.seconds
        const val BOUND_SECONDS = 10L
    }
}
