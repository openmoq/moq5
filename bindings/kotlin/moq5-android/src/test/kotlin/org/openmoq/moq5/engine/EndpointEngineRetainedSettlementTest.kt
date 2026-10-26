package org.openmoq.moq5.engine

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.ObjectRejectedException
import org.openmoq.moq5.SenderClosedException
import org.openmoq.moq5.SenderInterruptedException
import org.openmoq.moq5.WriteOutcome
import kotlin.test.Test
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The retained entry's TERMINAL settlement, pinned on the production
 * [Retained] state machine and the engine's own [EndpointEngine.retireRetained]
 * wrapper -- the exact pair the sender's retry uses.
 *
 * An attempt that ran has a terminal outcome: a committed value, or a typed
 * failure. That outcome must be stored under the entry lock and observed
 * identically by every later reader -- a retirement that arrives while the
 * attempt is in flight, one that arrives after the attempt settled but
 * before the worker published the caller's result, a repeated retirement,
 * the owner's close, and a quarantine. Only "nothing happened natively"
 * (no progress) leaves the entry retryable and lets the caller's own
 * cancellation or timeout stand.
 *
 * Scope, stated exactly: these rows drive the state machine and the engine
 * wrapper directly, because the window between `endAttempt` and the worker's
 * `result` publication is internal and has no public seam. They are NOT a
 * public or native interleaving replay. The public coupling for the
 * in-flight boundary is `PublicRetainedOwnershipTest`'s refusal rows, which
 * reach this same settlement through `MediaSender.write` / `endTrack`.
 */
internal class EndpointEngineRetainedSettlementTest : EngineFixture() {

    private fun entry() =
        Retained<WriteAttempt>(
            owner = Any(),
            attempt = { _, _ -> null },
            onOwnerRetired = { Result.failure(SenderClosedException(null)) },
        )

    /**
     * Read a result that must ALREADY have been published synchronously by
     * the call under test. The completion is asserted first, by name, so an
     * omitted delivery fails the row instead of parking it: nothing here
     * waits for a future event.
     */
    private suspend fun <T : Any> CompletableDeferred<T>.publishedResult(what: String): Result<T> {
        assertTrue(isCompleted, "$what: the result was never published, so there is nothing to read -- a missing delivery must fail by name, never hang the run")
        return runCatching { await() }
    }

    /** The worker's own order in `serviceOne`'s catch: retire, end the attempt, then publish. */
    private fun Retained<WriteAttempt>.failAttempt(failure: Throwable) {
        assertTrue(beginAttempt(), "setup: the attempt is claimed")
        retire()
        endAttempt(failure)
    }

    @Test
    fun `a typed failure survives a retirement arriving after settlement and before the result is published`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            val work = entry()
            val failure = ObjectRejectedException(MoqResultCode(-2))
            work.failAttempt(failure)
            assertTrue(!work.result.isCompleted, "setup: the worker has not published the caller's result yet")
            val actual = runCatching { engine.retireRetained(work) }
            assertSame(failure, actual.exceptionOrNull(), "the settled typed failure must reach a retirement arriving in this window, not be discarded as no-progress; got $actual")
        }

    @Test
    fun `an interruption refusal survives the same late boundary`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            val work = entry()
            val failure = SenderInterruptedException()
            work.failAttempt(failure)
            val actual = runCatching { engine.retireRetained(work) }
            assertSame(failure, actual.exceptionOrNull(), "the exact instance, not a class match; got $actual")
        }

    @Test
    fun `a committed transfer survives the same late boundary`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            val work = entry()
            val value = WriteAttempt(WriteOutcome.Accepted)
            assertTrue(work.beginAttempt())
            work.commit(value)
            work.endAttempt(null)
            assertTrue(!work.result.isCompleted, "setup: the result is not published yet")
            assertSame(value, engine.retireRetained(work), "a transfer that happened is always reported")
        }

    @Test
    fun `no progress at the same late boundary leaves the caller its own cancellation`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            val work = entry()
            assertTrue(work.beginAttempt())
            work.endAttempt(null)
            assertNull(engine.retireRetained(work), "nothing happened natively: the caller's cancellation or timeout stands")
        }

    @Test
    fun `the ORIGINAL in-flight waiter receives the exact failure instance, and a later retirement agrees`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            val work = entry()
            val failure = ObjectRejectedException(MoqResultCode(-2))
            assertTrue(work.beginAttempt())
            // The retirement subscribes while the attempt is still in flight:
            // THIS deferred is the one the contested caller awaits, so it is
            // the one the row reads. A later retirement is not its oracle.
            val waiting = assertIs<Retained.Retirement.AfterSettle<WriteAttempt>>(work.retire(), "setup: the in-flight retirement subscribes")
            assertTrue(!waiting.settled.isCompleted, "setup: the original waiter is still waiting for this attempt")
            work.endAttempt(failure)
            val delivered = waiting.settled.publishedResult("the original in-flight waiter")
            val settled = assertIs<Retained.Settled.Failed>(delivered.getOrThrow(), "the original waiter settles as the attempt's typed failure")
            assertSame(failure, settled.failure, "the ORIGINAL waiter must receive the exact recorded failure instance, not another of the same class and code")
            // And, separately, a later retirement through the engine wrapper
            // reports the same stored outcome.
            val late = runCatching { engine.retireRetained(work) }
            assertSame(failure, late.exceptionOrNull(), "a later retirement agrees with the original waiter; got $late")
        }

    @Test
    fun `repeated retirement after settlement answers identically`() =
        runBlocking {
            val (engine, _) = newEngine()
            engine.awaitConstructed()
            val work = entry()
            val failure = ObjectRejectedException(MoqResultCode(-2))
            work.failAttempt(failure)
            val first = runCatching { engine.retireRetained(work) }
            val second = runCatching { engine.retireRetained(work) }
            assertSame(failure, first.exceptionOrNull(), "first retirement: $first")
            assertSame(failure, second.exceptionOrNull(), "a repeated retirement answers the same terminal outcome, never a fresh no-progress; got $second")

            val committed = entry()
            val value = WriteAttempt(WriteOutcome.Accepted)
            assertTrue(committed.beginAttempt())
            committed.commit(value)
            committed.endAttempt(null)
            assertSame(value, engine.retireRetained(committed))
            assertSame(value, engine.retireRetained(committed), "a repeated retirement reports the same transfer")
        }

    @Test
    fun `a failed attempt is terminal -- no further attempt may be claimed, while no progress stays retryable`() =
        runBlocking {
            val failed = entry()
            assertTrue(failed.beginAttempt(), "setup: claimed")
            failed.endAttempt(ObjectRejectedException(MoqResultCode(-2)))
            assertTrue(!failed.beginAttempt(), "a settled failure is terminal: the entry must not revert to a retryable idle state")

            val idle = entry()
            assertTrue(idle.beginAttempt(), "setup: claimed")
            idle.endAttempt(null)
            assertTrue(idle.beginAttempt(), "no progress alone leaves the entry retryable while its caller has not retired")
        }

    @Test
    fun `owner retirement never overwrites a committed transfer`() =
        runBlocking {
            val work = entry()
            val value = WriteAttempt(WriteOutcome.Accepted)
            assertTrue(work.beginAttempt())
            work.commit(value)
            work.endAttempt(null)
            // The owner closes in the window before the worker published the result.
            work.resolveOwnerRetired()
            val delivered = work.result.publishedResult("owner retirement over a committed transfer")
            assertSame(value, delivered.getOrNull(), "an owner close must not replace a transfer that already happened with its close answer; got $delivered")
        }

    @Test
    fun `owner retirement never overwrites a typed failure`() =
        runBlocking {
            val work = entry()
            val failure = ObjectRejectedException(MoqResultCode(-2))
            assertTrue(work.beginAttempt())
            work.endAttempt(failure)
            work.resolveOwnerRetired()
            val delivered = work.result.publishedResult("owner retirement over a typed failure")
            assertSame(failure, delivered.exceptionOrNull(), "an owner close must not replace the attempt's own typed failure; got $delivered")
        }

    /**
     * Added with the correction: `resolveQuarantined` did not exist before
     * it, so this row has no RED against the previous hashes. Its mutant
     * (the stored-outcome check removed) is what proves it discriminates.
     */
    @Test
    fun `quarantine never overwrites a committed transfer or a typed failure, and wins only when no attempt won`() =
        runBlocking {
            val bridge = BridgeContractException(BridgeFailure("quarantine-consistency", -1))

            val committed = entry()
            val value = WriteAttempt(WriteOutcome.Accepted)
            assertTrue(committed.beginAttempt())
            committed.commit(value)
            committed.endAttempt(null)
            committed.resolveQuarantined(bridge)
            assertSame(value, committed.result.publishedResult("quarantine over a committed transfer").getOrNull(), "a quarantine must not erase a transfer that already happened")

            val failed = entry()
            val refusal = ObjectRejectedException(MoqResultCode(-2))
            assertTrue(failed.beginAttempt())
            failed.endAttempt(refusal)
            failed.resolveQuarantined(bridge)
            assertSame(refusal, failed.result.publishedResult("quarantine over a typed refusal").exceptionOrNull(), "a quarantine must not replace the attempt's own typed refusal")

            val idle = entry()
            assertTrue(idle.beginAttempt())
            idle.endAttempt(null)
            idle.resolveQuarantined(bridge)
            assertSame(bridge, idle.result.publishedResult("quarantine with no attempt of its own").exceptionOrNull(), "with no attempt of its own, the stable bridge failure is the answer")
        }

    @Test
    fun `owner retirement answers with the close outcome when no attempt won`() =
        runBlocking {
            val work = entry()
            assertTrue(work.beginAttempt())
            work.endAttempt(null)
            work.resolveOwnerRetired()
            val delivered = work.result.publishedResult("owner retirement with no attempt of its own")
            val t = delivered.exceptionOrNull()
            if (t !is SenderClosedException) fail("nothing happened natively, so the owner-close answer stands; got $delivered")
        }
}
