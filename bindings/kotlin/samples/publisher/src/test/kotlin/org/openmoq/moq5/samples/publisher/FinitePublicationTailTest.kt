package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.job
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.openmoq.moq5.DrainOutcome
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A finite publication is published only when the media it accepted -- every
 * object and both track terminals -- has drained BEFORE completion is
 * requested, and the terminal catalog that completion records has drained
 * after it. Completion removes the tracks and discards what they still hold,
 * so draining only after it would lose that tail.
 *
 * These rows drive the real session, owner and live publication; only the
 * sender boundary and the endpoint's drain answers are scripted. The handle
 * inventory is the ownership's settlement: a publication that is not
 * published retires both handles; a published one releases them.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class FinitePublicationTailTest {

    private suspend fun kotlinx.coroutines.test.TestScope.publishOneFrame(rig: Rig): BroadcastOutcome {
        val (video, audio) = CompletableDeferred(validatedVideo()) to CompletableDeferred(validatedAudio())
        val result =
            withRun({ rig.run(rig.formatPublication(FormatSources(video, audio), BUDGET)) }) { owner ->
                runCurrent()
                deliver(rig.video, 0, VIDEO_ANNEXB.copyOf(), 33_333L)
                runCurrent()
                deliverEnd(rig.video, 1)
                deliverEnd(rig.audio, 0)
                runCurrent()
                settled(owner)
            }
        return result.getOrThrow()
    }

    @Test
    fun `u1 both barriers flushed publish, in order, and the handles are released`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        assertIs<BroadcastOutcome.Published>(publishOneFrame(rig))
        assertEquals(listOf("drain", "complete", "drain"), rig.completion.calls, "the media drain, the completion, the catalog drain")
        assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a"), rig.tracks.labels(), "released, not removed")
    }

    @Test
    fun `u2 an unflushed media drain never requests completion, and the handles are retired`() = runTest {
        for (answer in NOT_FLUSHED) {
            val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
            rig.completion.answers.addLast(answer)
            val outcome = publishOneFrame(rig)
            val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome, "$answer")
            assertEquals("the accepted media did not drain before completion: $answer", unfinished.reason)
            assertEquals(listOf("drain"), rig.completion.calls, "$answer: no completion")
            assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a", "remove:v", "remove:a"), rig.tracks.labels(), "$answer: both handles retired once")
            assertTrue(rig.owner.isDecided)
        }
    }

    @Test
    fun `u3 an unflushed catalog drain is unfinished, the accepted completion stands, and the handles are retired`() = runTest {
        for (answer in NOT_FLUSHED) {
            val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
            rig.completion.answers.addLast(DrainOutcome.Flushed)
            rig.completion.answers.addLast(answer)
            val outcome = publishOneFrame(rig)
            val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome, "$answer")
            assertEquals("the completion was accepted but its terminal catalog did not drain: $answer", unfinished.reason)
            assertEquals(listOf("drain", "complete", "drain"), rig.completion.calls, "$answer: one completion, not retried")
            assertEquals(listOf("add:video", "add:audio", "write:v[11b@MediaMicros(33333)]", "end:v", "end:a", "remove:v", "remove:a"), rig.tracks.labels(), "$answer: both handles retired once")
            assertTrue(rig.owner.isDecided)
        }
    }

    /** The shared ending itself, over a recording sink: what it asks for, in order, and what it says. */
    private class Recording(private val answers: List<DrainOutcome>, private val onComplete: suspend () -> Unit = {}) : CompletionSink {
        val calls = mutableListOf<String>()
        private var drains = 0

        override suspend fun complete() {
            calls.add("complete")
            onComplete()
        }

        override suspend fun drain(timeout: Duration): DrainOutcome {
            calls.add("drain($timeout)")
            return answers.getOrNull(drains++) ?: throw AssertionError("an unscripted drain after $calls")
        }
    }

    @Test
    fun `h1 the shared ending drains the media, completes, then drains the catalog, and says which drain it was`() = runTest {
        val sink = Recording(listOf(DrainOutcome.Flushed, DrainOutcome.Flushed))
        val said = mutableListOf<String>()
        assertSame(Finish.Flushed, sink.finishPublication(BUDGET) { said.add(it) })
        assertEquals(listOf("drain($BUDGET)", "complete", "drain($BUDGET)"), sink.calls)
        assertEquals(listOf("drain (accepted media and track terminals): Flushed", "drain (terminal catalog): Flushed"), said)
    }

    @Test
    fun `h2 the shared ending stops at whichever barrier did not flush`() = runTest {
        for (answer in NOT_FLUSHED) {
            val first = Recording(listOf(answer))
            assertEquals(Finish.MediaNotDrained(answer), first.finishPublication(BUDGET) {})
            assertEquals(listOf("drain($BUDGET)"), first.calls, "$answer: no completion after an unflushed media drain")
            val second = Recording(listOf(DrainOutcome.Flushed, answer))
            assertEquals(Finish.CompletionNotDrained(answer), second.finishPublication(BUDGET) {})
            assertEquals(listOf("drain($BUDGET)", "complete", "drain($BUDGET)"), second.calls, "$answer: one completion, not retried")
        }
    }

    @Test
    fun `h3 a cancellation that arrives during completion starts no catalog drain`() = runTest {
        val why = CancellationException("stopped during completion")
        val sink = Recording(listOf(DrainOutcome.Flushed, DrainOutcome.Flushed)) { currentCoroutineContext().job.cancel(why) }
        val raised = runCatching { kotlinx.coroutines.coroutineScope { sink.finishPublication(BUDGET) {} } }.exceptionOrNull()
        assertIs<CancellationException>(raised)
        assertEquals(listOf("drain($BUDGET)", "complete"), sink.calls, "the completion stands; the catalog drain never begins")
    }

    private companion object {
        val BUDGET = 5.seconds
        val NOT_FLUSHED = listOf(DrainOutcome.TimedOut, DrainOutcome.Interrupted, DrainOutcome.Closed, DrainOutcome.Unsupported)
    }
}
