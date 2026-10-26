package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.async
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class PlaybackReceiveLoopTest {
    private class Render : PlaybackRenderPort {
        val offered = mutableListOf<AccessUnitOutcome.Unit>()
        val times = mutableListOf<Long>()
        var busy = false
        var pumpCount = 0
        var endedCount = 0
        var pumpFailure: Throwable? = null
        var eosOnEndInputs = true
        override var decoderOutputsEnded = false
        override var videoOutputsReleased = 0
        override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long): PlaybackOffer {
            offered.add(unit); times.add(logicalUs)
            return if (busy) PlaybackOffer.Busy else PlaybackOffer.Queued
        }
        override fun pump(): Boolean { pumpCount++; pumpFailure?.let { throw it }; return false }
        override fun endInputs() { endedCount++; if (eosOnEndInputs) decoderOutputsEnded = true }
    }
    private val unit = AccessUnitOutcome.Unit(byteArrayOf(1), 10, true)
    private fun packet(lane: PlaybackLane = PlaybackLane.Video) = PlaybackReceived(lane, listOf(PlaybackTimedUnit(unit, 30)))
    private suspend fun settled(task: Job, checks: suspend () -> Unit) {
        try { checks() } finally { withContext(NonCancellable) { task.cancel(); task.join() } }
    }

    @Test fun finiteBudgetEndsInputsAndAwaitsDecoderEosWithoutClaimingReceiverEnd() = runTest {
        val renderer = Render(); renderer.eosOnEndInputs = false; var reads = 0
        val result = async { receivePlayback({ reads++; packet() }, renderer, PlaybackFailure(), false, 2, waitLimitMs = 1_000) }
        settled(result) {
            runCurrent()
            assertEquals(2, reads); assertEquals(1, renderer.endedCount)
            assertFalse(result.isCompleted, "no success before decoder output EOS")
            advanceTimeBy(900); runCurrent()
            assertFalse(result.isCompleted); assertEquals(1, renderer.endedCount)
            renderer.decoderOutputsEnded = true
            advanceTimeBy(3); runCurrent()
            assertEquals(PlaybackReadResult(PlaybackReadEnd.ObjectLimit, 2, 2), withTimeout(100) { result.await() })
            assertEquals(2, reads); assertEquals(1, renderer.endedCount)
            assertEquals(listOf(30L, 30L), renderer.times)
        }
    }

    @Test fun finiteBudgetDecoderEosHasTheNamedWaitBound() = runTest {
        val renderer = Render(); renderer.eosOnEndInputs = false
        val result = async { runCatching { receivePlayback({ packet() }, renderer, PlaybackFailure(), false, 1, waitLimitMs = 20) } }
        settled(result) {
            runCurrent(); assertEquals(1, renderer.endedCount)
            advanceTimeBy(19); runCurrent(); assertFalse(result.isCompleted)
            advanceTimeBy(2); runCurrent()
            val raised = withTimeout(100) { result.await() }.exceptionOrNull()
            assertTrue(raised is PlaybackRenderRefused, "got $raised")
            assertEquals("decoder EOS did not arrive within the sample's wait bound", raised.message)
            assertTrue(raised.suppressed.isEmpty()); assertEquals(1, renderer.endedCount)
            val count = renderer.pumpCount; advanceTimeBy(100); runCurrent(); assertEquals(count, renderer.pumpCount)
        }
    }

    @Test fun callbackDuringFiniteBudgetDecoderTailIsNotSuccess() = runTest {
        val renderer = Render(); renderer.eosOnEndInputs = false; val failure = PlaybackFailure()
        val boom = AssertionError("decoder callback during tail")
        val result = async { runCatching { receivePlayback({ packet() }, renderer, failure, false, 1) } }
        settled(result) {
            runCurrent(); assertEquals(1, renderer.endedCount); assertFalse(result.isCompleted)
            failure.record(boom); renderer.decoderOutputsEnded = true
            advanceTimeBy(3); runCurrent()
            assertSame(boom, withTimeout(100) { result.await() }.exceptionOrNull())
            assertTrue(boom.suppressed.isEmpty()); assertEquals(1, renderer.endedCount)
            val count = renderer.pumpCount; advanceTimeBy(100); runCurrent(); assertEquals(count, renderer.pumpCount)
        }
    }

    @Test fun callbackEndsFiniteBudgetDecoderTailWithoutWaitingForItsBound() = runTest {
        val renderer = Render(); renderer.eosOnEndInputs = false; val failure = PlaybackFailure()
        val boom = AssertionError("decoder callback while EOS is outstanding")
        val result = async { runCatching { receivePlayback({ packet() }, renderer, failure, false, 1, waitLimitMs = 10_000) } }
        settled(result) {
            runCurrent(); assertEquals(1, renderer.endedCount); assertFalse(result.isCompleted)
            failure.record(boom)
            advanceTimeBy(3); runCurrent()
            assertTrue(result.isCompleted, "a callback must end the decoder-EOS wait, not its wait bound")
            assertSame(boom, result.await().exceptionOrNull())
            assertTrue(boom.suppressed.isEmpty()); assertFalse(renderer.decoderOutputsEnded)
        }
    }

    @Test fun callerCancellationDuringFiniteBudgetDecoderTailKeepsRecordedCallback() = runTest {
        val renderer = Render(); renderer.eosOnEndInputs = false; val failure = PlaybackFailure()
        val boom = AssertionError("callback before caller stop"); val receipt = CompletableDeferred<Throwable?>()
        val work = launch { receipt.complete(runCatching { receivePlayback({ packet() }, renderer, failure, false, 1) }.exceptionOrNull()) }
        settled(work) {
            runCurrent(); assertEquals(1, renderer.endedCount); assertFalse(receipt.isCompleted)
            failure.record(boom); work.cancel(CancellationException("caller stop during decoder tail")); work.join()
            val raised = withTimeout(100) { receipt.await() }
            assertTrue(raised is CancellationException, "no invented success: got $raised")
            assertTrue(raised.suppressed.any { it === boom }, "caller cancellation must not lose the recorded callback")
            assertEquals(1, renderer.endedCount)
            val count = renderer.pumpCount; advanceTimeBy(100); runCurrent(); assertEquals(count, renderer.pumpCount)
        }
    }

    @Test fun onlyReceiverNullSignalsDecoderEosAndUnrelatedObjectsDoNotConsumeBudget() = runTest {
        val renderer = Render()
        val packets = ArrayDeque(listOf(PlaybackReceived(null, emptyList()), packet()))
        val result = receivePlayback({ packets.removeFirstOrNull() }, renderer, PlaybackFailure(), false, 10)
        assertEquals(PlaybackReadResult(PlaybackReadEnd.ReceiverEnded, 1, 1), result)
        assertEquals(1, renderer.endedCount)
    }

    @Test fun callbackFailureWakesParkedReadAndSettlesItBeforeReturning() = runTest {
        for (raised in listOf(IllegalStateException("callback"), AssertionError("callback"), CancellationException("callback"))) {
            val renderer = Render(); val failure = PlaybackFailure(); var settled = false
            val entered = CompletableDeferred<Unit>()
            val result = async { runCatching { receivePlayback({ entered.complete(Unit); try { awaitCancellation() } finally { settled = true } },
                renderer, failure, false, 10) } }
            settled(result) {
                withTimeout(100) { entered.await() }; failure.record(raised)
                assertSame(raised, withTimeout(100) { result.await() }.exceptionOrNull()); assertTrue(settled)
                assertTrue(raised.suppressed.isEmpty(), "ordinary reader cancellation is not failed cleanup")
                val count = renderer.pumpCount; advanceTimeBy(100); runCurrent(); assertEquals(count, renderer.pumpCount)
            }
        }
    }

    @Test fun pumpFailureWakesParkedReadWithoutAnotherObject() = runTest {
        val renderer = Render(); val boom = AssertionError("decoder"); var settled = false
        val entered = CompletableDeferred<Unit>()
        val result = async { runCatching { receivePlayback({ entered.complete(Unit); try { awaitCancellation() } finally { settled = true } },
            renderer, PlaybackFailure(), false, 10) } }
        settled(result) {
            withTimeout(100) { entered.await() }; renderer.pumpFailure = boom
            advanceTimeBy(3); runCurrent()
            assertSame(boom, withTimeout(100) { result.await() }.exceptionOrNull()); assertTrue(settled)
        }
    }

    @Test fun busyRetainsExactUnitAndDoesNotReadAhead() = runTest {
        val renderer = Render(); renderer.busy = true; var reads = 0
        val result = async { receivePlayback({ reads++; packet() }, renderer, PlaybackFailure(), false, 1) }
        settled(result) {
            runCurrent(); assertEquals(1, reads); assertFalse(result.isCompleted)
            advanceTimeBy(5); runCurrent(); renderer.busy = false
            advanceTimeBy(3); runCurrent()
            assertEquals(PlaybackReadEnd.ObjectLimit, withTimeout(100) { result.await() }.end)
            assertEquals(1, reads); assertTrue(renderer.offered.size > 1)
            renderer.offered.forEach { assertSame(unit, it) }
        }
    }

    @Test fun absentSelectedAudioHasAFiniteNamedStartupRefusal() = runTest {
        val renderer = Render(); var reads = 0; var settled = false
        val result = async { runCatching { receivePlayback({ if (reads++ == 0) packet() else try { awaitCancellation() } finally { settled = true } },
            renderer, PlaybackFailure(), true, 10, waitLimitMs = 20) } }
        settled(result) {
            runCurrent(); advanceTimeBy(21); runCurrent()
            val raised = withTimeout(100) { result.await() }.exceptionOrNull()
            assertTrue(raised is PlaybackRenderRefused); assertTrue(settled)
            assertTrue(raised.suppressed.isEmpty(), "a clean deadline cancellation has no failed cleanup")
            assertEquals(0, renderer.endedCount)
        }
    }

    @Test fun callerCancellationStopsReaderAndPumpWithoutEos() = runTest {
        val renderer = Render(); var settled = false; val entered = CompletableDeferred<Unit>()
        val result = async { receivePlayback({ entered.complete(Unit); try { awaitCancellation() } finally { settled = true } },
            renderer, PlaybackFailure(), false, 10) }
        settled(result) {
            withTimeout(100) { entered.await() }; result.cancel(); result.join()
            assertTrue(settled); assertEquals(0, renderer.endedCount)
            val count = renderer.pumpCount; advanceTimeBy(100); runCurrent(); assertEquals(count, renderer.pumpCount)
        }
    }

    @Test fun unexpectedReaderCleanupIsNotLostBehindCallbackFailure() = runTest {
        val renderer = Render(); val failure = PlaybackFailure()
        val boom = AssertionError("callback"); val cleanup = AssertionError("read cleanup")
        val entered = CompletableDeferred<Unit>()
        val result = async { runCatching { receivePlayback({ entered.complete(Unit); try { awaitCancellation() } finally { throw cleanup } },
            renderer, failure, false, 10) } }
        settled(result) {
            withTimeout(100) { entered.await() }; failure.record(boom)
            val actual = withTimeout(100) { result.await() }.exceptionOrNull()
            assertSame(boom, actual); assertEquals(listOf(cleanup), actual!!.suppressed.toList())
        }
    }

    @Test fun startupDeadlineStillReportsReaderCleanupFailure() = runTest {
        val cleanup = AssertionError("deadline reader cleanup")
        val result = runCatching { withTimeout(100) { receivePlayback({ try { awaitCancellation() } finally { throw cleanup } },
            Render(), PlaybackFailure(), true, 10, waitLimitMs = 20) } }
        val failure = result.exceptionOrNull()
        assertTrue(failure is PlaybackRenderRefused)
        assertTrue(failure.suppressed.any { it === cleanup }, "deadline must not erase failed reader cleanup")
    }

    @Test fun callbackAfterStartupWakesReadWithoutRelyingOnStartupTimeout() = runTest {
        val failure = PlaybackFailure(); var reads = 0; var settled = false
        val entered = CompletableDeferred<Unit>(); val boom = AssertionError("after startup")
        val result = async { runCatching { receivePlayback({ if (reads++ == 0) packet() else {
            entered.complete(Unit); try { awaitCancellation() } finally { settled = true }
        } }, Render(), failure, false, 10, waitLimitMs = 10000) } }
        settled(result) {
            withTimeout(100) { entered.await() }; failure.record(boom)
            val answer = withTimeout(100) { result.await() }
            assertSame(boom, answer.exceptionOrNull()); assertTrue(settled)
        }
    }

    @Test fun badBudgetIsRejectedBeforeReaderOrRendererEffects() = runTest {
        val renderer = Render()
        assertFailsWith<IllegalArgumentException> { receivePlayback({ error("must not read") }, renderer, PlaybackFailure(), false, 0) }
        assertEquals(0, renderer.pumpCount); assertTrue(renderer.offered.isEmpty())
    }

    @Test fun callerCancellationPreservesRealReaderCleanupFailure() = runTest {
        val cleanup = AssertionError("caller-cancelled reader cleanup")
        val entered = CompletableDeferred<Unit>(); val receipt = CompletableDeferred<Throwable?>()
        val work = launch {
            receipt.complete(runCatching { receivePlayback({ entered.complete(Unit); try { awaitCancellation() } finally { throw cleanup } },
                Render(), PlaybackFailure(), false, 10) }.exceptionOrNull())
        }
        settled(work) {
            withTimeout(100) { entered.await() }; work.cancel(CancellationException("caller stop")); work.join()
            val raised = withTimeout(100) { receipt.await() }
            assertTrue(raised is CancellationException)
            assertTrue(raised.suppressed.any { it === cleanup }, "escaping cancellation must retain the original cleanup")
        }
    }

    @Test fun callerCancellationDuringReadSettlementPreservesRecordedCallback() = runTest {
        val failure = PlaybackFailure(); val boom = AssertionError("callback")
        val entered = CompletableDeferred<Unit>(); val closing = CompletableDeferred<Unit>(); val finish = CompletableDeferred<Unit>()
        val receipt = CompletableDeferred<Throwable?>()
        val work = launch {
            receipt.complete(runCatching { receivePlayback({ entered.complete(Unit); try { awaitCancellation() } finally {
                withContext(NonCancellable) { closing.complete(Unit); finish.await() }
            } }, Render(), failure, false, 10) }.exceptionOrNull())
        }
        settled(work) {
            try {
                withTimeout(100) { entered.await() }; failure.record(boom); withTimeout(100) { closing.await() }
                work.cancel(CancellationException("caller stop while settling")); finish.complete(Unit); work.join()
                val raised = withTimeout(100) { receipt.await() }
                assertTrue(raised is CancellationException)
                assertTrue(raised.suppressed.any { it === boom }, "caller cancellation must not replace the owned callback cause")
            } finally { finish.complete(Unit) }
        }
    }
}
