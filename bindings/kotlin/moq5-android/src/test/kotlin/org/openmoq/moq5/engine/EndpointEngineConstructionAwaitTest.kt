package org.openmoq.moq5.engine

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Work accepted BEFORE construction must be resolved when construction fails.
 *
 * The engine accepts a `perform()` while the worker is still inside the
 * factory. If the factory then throws, the worker retires without draining the
 * queue, and that caller is never resumed -- a strand, not a failure. The
 * contract pinned here: every job accepted before a failed construction is
 * resolved with the EXACT construction cause, and no backend call is made.
 *
 * The wait is bounded through cancellation, which `perform()` honours for a
 * queued job, so a stranding engine fails this row by name rather than hanging
 * the suite.
 */
internal class EndpointEngineConstructionAwaitTest : EngineFixture() {
    private companion object {
        const val BOUND_MILLIS = 2_000L
        const val SETTLE_MILLIS = 100L

        /**
         * Enough repetitions for the close to win the race to the worker's
         * pre-creation phase check in practice. The verdict never depends on
         * which side is taken: the invariant holds on both.
         */
        const val RACE_ITERATIONS = 200
    }

    /**
     * A CLOSE BEFORE CREATION must make `awaitConstructed()` throw the same
     * `EngineClosedException` the worker records -- never report success for
     * a root that was never created.
     *
     * Only a waiter registered BEFORE the decision can observe how the
     * decision is delivered; one arriving after it reads the stored outcome.
     * So the waiter is started UNDISPATCHED: `async` returns only once
     * `awaitConstructed()` has run to its suspension point on this thread,
     * which under the engine lock either registered the waiter or found the
     * construction already decided. The close follows. Whenever the worker's
     * pre-creation check then loses to that close, the registered waiter is
     * exactly the one the 1384 defect resumed with `null`.
     *
     * The worker's phase check precedes the factory, so nothing can hold it
     * deterministically without a seam this engine does not have. The row is
     * a conservation law over the race -- whichever side is taken, the
     * outcome must be coherent with whether the factory ran -- and it FAILS
     * if the pre-creation side was never taken, so a scheduler that never
     * exercises the defect's window is a named coverage failure, not a pass.
     */
    @Test
    fun `awaitConstructed reports a close before creation as closed, never as constructed`() =
        runBlocking {
            var preCreationCloses = 0
            repeat(RACE_ITERATIONS) {
                val backend = newBackend()
                var factoryRan = false
                val (e, b) =
                    newEngine(backend) {
                        factoryRan = true
                        backend
                    }
                // Registered (or decided) before close() is even called.
                val waiter =
                    async(start = CoroutineStart.UNDISPATCHED) {
                        try {
                            e.awaitConstructed()
                            null
                        } catch (t: Throwable) {
                            t
                        }
                    }
                e.close()
                val outcome = withTimeoutOrNull(BOUND_MILLIS) { waiter.await() }
                if (outcome == null && waiter.isActive) {
                    waiter.cancel()
                    throw AssertionError("a pre-registered construction waiter was never resolved")
                }
                assertTrue(e.isBackendRetired, "the engine must retire after close")
                if (factoryRan) {
                    // The root was created and, being owned, torn down.
                    assertTrue(outcome == null, "a created root is a constructed root; saw $outcome")
                    assertEquals(listOf("stop", "destroy"), b.observed().callOrder)
                } else {
                    preCreationCloses++
                    assertTrue(
                        outcome is EngineClosedException,
                        "closed before creation: a waiter registered before the decision must be " +
                            "resumed with EngineClosedException, not success; saw $outcome",
                    )
                    assertEquals(emptyList(), b.observed().callOrder, "no root, no teardown")
                }
            }
            println("pre-creation closes observed: $preCreationCloses of $RACE_ITERATIONS")
            assertTrue(
                preCreationCloses > 0,
                "COVERAGE: the close never beat the worker to its pre-creation check in " +
                    "$RACE_ITERATIONS iterations, so the pre-registered-waiter window was not " +
                    "exercised and this row proves nothing about it",
            )
        }

    @Test
    fun `a job accepted before a failed construction resolves with the exact construction cause`() =
        runBlocking {
            val backend = newBackend()
            val boom = IllegalStateException("native create refused")
            val entered = gate("factory-entered")
            val release = gate("factory-release")
            val (e, b) =
                newEngine(backend) {
                    entered.open()
                    release.await()
                    throw boom
                }
            assertTrue(entered.await(), "the worker never reached the factory")
            markQuarantined(e)

            // Accepted while the factory is still running, from another
            // thread so this row can release the factory afterwards.
            var outcome: Throwable? = null
            val submitted = gate("perform-submitted")
            val caller =
                async(Dispatchers.Default) {
                    submitted.open()
                    try {
                        e.perform { }
                        "completed"
                    } catch (t: Throwable) {
                        outcome = t
                        "failed"
                    }
                }
            assertTrue(submitted.await(), "the caller never submitted")
            // Let the queue take it before the factory fails.
            Thread.sleep(SETTLE_MILLIS)
            release.open()
            val resolution = withTimeoutOrNull(BOUND_MILLIS) { caller.await() }
            if (resolution == null) caller.cancel()
            assertTrue(
                resolution != null,
                "a job accepted before a failed construction was never resolved within $BOUND_MILLIS ms: stranded",
            )
            // Coroutine stack-trace recovery may hand a caller a copy whose
            // cause is the original; either way the cause is EXACTLY boom.
            val seen = outcome
            assertTrue(
                seen === boom || seen?.cause === boom,
                "the job must learn the exact construction cause, saw $seen",
            )
            assertEquals(emptyList(), b.observed().callOrder, "no root, so no backend call")
        }
}
