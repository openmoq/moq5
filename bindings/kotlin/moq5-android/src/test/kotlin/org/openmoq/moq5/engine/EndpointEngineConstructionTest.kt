package org.openmoq.moq5.engine

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The two CANCELLATION WINDOWS of asynchronous construction.
 *
 * The engine takes a factory and invokes it ON THE WORKER, before that worker
 * performs any endpoint operation, so one exact thread creates, waits,
 * snapshots, stops and destroys the root. That leaves two windows in which a
 * close can arrive, and they owe OPPOSITE things:
 *
 *  * BEFORE native creation -- no root exists, so nothing may be torn down. A
 *    stop or destroy here would address an object that was never created.
 *  * AFTER native ownership but BEFORE publication -- the root exists, so it
 *    MUST be torn down. Skipping it would leak the endpoint with no owner able
 *    to reach it again.
 *
 * The first window has a DETERMINISTIC representative -- a factory that fails,
 * so no root ever exists -- and the second is driven by gating the factory
 * after it has produced its root. The race BETWEEN them cannot be scheduled, so
 * it is covered by a conservation law that holds on both sides of it.
 *
 * Every row additionally asserts that no worker, waiter or continuation is left
 * behind: the engine retires and every close resolves exactly once.
 */
internal class EndpointEngineConstructionTest : EngineFixture() {
    @Test
    fun `a factory failure fails every waiter and makes no backend call`() =
        runTest {
            val backend = newBackend()
            val boom = IllegalStateException("native create refused")
            val entered = gate("factory-entered")
            val (e, b) =
                newEngine(backend) {
                    entered.open()
                    throw boom
                }
            // Without this the close can win the race against the factory and
            // the row would silently become a different window.
            assertTrue(entered.await(), "the worker never reached the factory")
            // A construction failure is terminal in the same way a quarantine
            // is: cleanup must audit this engine, not demand a later successful
            // close. Armed only once the failure is established.
            markQuarantined(e)

            val thrown =
                try {
                    e.close()
                    null
                } catch (t: Throwable) {
                    t
                }
            assertTrue(thrown === boom, "the waiter must learn the construction cause, saw $thrown")
            assertTrue(e.isBackendRetired, "the engine must retire")
            assertEquals(
                emptyList(),
                b.observed().callOrder,
                "a root that was never created cannot be stopped or destroyed",
            )

            val again =
                try {
                    e.close()
                    null
                } catch (t: Throwable) {
                    t
                }
            assertTrue(again === boom, "a later close must learn the same stable cause")
        }

    @Test
    fun `a close racing publication still destroys the root it owns`() =
        runTest {
            val backend = newBackend()
            val produced = gate("root-produced")
            val release = gate("publish-release")

            // The factory has ALREADY produced the root when the close lands,
            // so this is the second window: native ownership exists and the
            // engine owes it a teardown.
            val (e, b) =
                newEngine(backend) {
                    produced.open()
                    release.await()
                    backend
                }
            assertTrue(produced.await(), "the worker never reached the factory")

            // The boundary is "close() reached its first suspension", not "the
            // helper is about to call close()": releasing the factory earlier
            // would let publication win the race and the row would stop
            // exercising the window it is named for.
            val entered = gate("close-entered")
            val closed =
                callBlocking("close-racing-publication") {
                    coroutineScope {
                        val child = launch(start = CoroutineStart.UNDISPATCHED) { e.close() }
                        entered.open()
                        child.join()
                    }
                }
            assertTrue(entered.await(), "the close never reached its first suspension")
            release.open()
            closed.assertCompleted() // exactly one resolution, and not a failure

            assertEquals(
                listOf("stop", "destroy"),
                b.observed().callOrder,
                "a root the engine owns must be stopped and destroyed exactly once, in order",
            )
            assertTrue(e.isBackendRetired, "the engine must retire")
        }

    @Test
    fun `an operation submitted before construction still runs on the one worker`() =
        runTest {
            val backend = newBackend()
            val entered = gate("factory-entered")
            val release = gate("factory-release")
            val (e, b) =
                newEngine(backend) {
                    entered.open()
                    release.await()
                    backend
                }
            assertTrue(entered.await(), "the worker never reached the factory")

            // The submission cannot be nudged natively yet -- there is no root.
            // It must still be accepted and, once the root exists, run.
            val ledger = ledger()
            assertEquals(SubmitOutcome.ACCEPTED, e.post { ledger.completed("early") })
            release.open()
            assertTrue(
                ledger.await("the early submission runs") { it.completed.contains("early") },
                "a job submitted before construction was stranded",
            )
            // The wake raised before the root existed is RETAINED, not dropped:
            // it is delivered exactly once as soon as the root can take it. A
            // dropped wake would leave the C tier's level unset even though
            // work was accepted, and only the engine's own level would carry it.
            assertEquals(
                1,
                b.observed().wakeCount,
                "the wake raised before construction must reach the root exactly once",
            )

            e.snapshot()
            assertTrue(
                b.observed().callOrder.contains("snapshot"),
                "the snapshot must be taken on the worker that created the root",
            )
        }

    @Test
    fun `closing at any point in the construction race leaves no partial teardown`() =
        runTest {
            // The window between the worker reaching the factory and publishing
            // the root cannot be scheduled deterministically, so the oracle is
            // a CONSERVATION law that holds at every point in it: either the
            // root was never created and nothing was torn down, or it was
            // created and torn down exactly once, in order. A partial teardown
            // -- a stop with no destroy, or a destroy with no stop -- is a leak
            // or a double free, and is what this row exists to catch.
            repeat(ITERATIONS) {
                val backend = newBackend()
                val calls = AtomicInteger(0)
                val (e, b) =
                    newEngine(backend) {
                        calls.incrementAndGet()
                        backend
                    }
                e.close()

                val order = b.observed().callOrder
                assertTrue(
                    order.isEmpty() || order == listOf("stop", "destroy"),
                    "teardown must be all or nothing, saw $order after ${calls.get()} factory calls",
                )
                assertTrue(e.isBackendRetired, "the engine must retire")
            }
        }

    private companion object {
        /**
         * Enough repetitions to visit both sides of the race in practice. The
         * verdict does not depend on which side is taken -- the conservation
         * law holds on both -- so this is coverage, never a timing oracle.
         */
        const val ITERATIONS = 64
    }
}
