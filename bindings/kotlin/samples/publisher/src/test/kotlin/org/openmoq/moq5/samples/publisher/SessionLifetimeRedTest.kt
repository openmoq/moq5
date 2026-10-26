package org.openmoq.moq5.samples.publisher

import java.io.IOException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The publisher's own resource lifetime: every resource it successfully acquires is closed, child before
 * endpoint, once each -- driven through [broadcastLifetime], the function
 * `BroadcastSession.broadcast` runs.
 *
 * The stubs SUSPEND and observe cancellation, because that is what
 * discriminates the interesting case: the library's closes go through the
 * engine's `perform`, which calls `ensureActive()`, so a close submitted from
 * an already-cancelled session need not run at all. A stub that neither
 * suspends nor checks cancellation cannot tell those apart.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class SessionLifetimeRedTest {

    private class Ops {
        val order = mutableListOf<String>()
        val barriers = mutableListOf<CompletableDeferred<Unit>>()

        fun record(step: String) {
            order.add(step)
        }

        /** A close that suspends, like the real one, and reports whether it ran under cancellation. */
        suspend fun close(name: String) {
            record("$name:close-begins")
            yield()
            record("$name:close-ends")
        }

        fun settle() {
            barriers.forEach { it.complete(Unit) }
        }
    }

    private fun ops(body: suspend TestScope.(Ops) -> Unit) =
        runTest {
            val o = Ops()
            try {
                body(o)
            } finally {
                o.settle()
            }
        }

    @Test
    fun p1_theOrdinaryPathClosesChildThenEndpointExactlyOnce() = ops { o ->
        broadcastLifetime(
            connect = { o.record("connect"); "endpoint" },
            describe = { o.record("describe") },
            attach = { o.record("attach"); "child" },
            run = { _, _ -> o.record("run") },
            closeChild = { o.close("child") },
            closeEndpoint = { o.close("endpoint") },
        )
        assertEquals(
            listOf("connect", "describe", "attach", "run", "child:close-begins", "child:close-ends", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
        )
    }

    @Test
    fun p2_aFailedAcquireCreatesNothingToClose() = ops { o ->
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); throw IllegalStateException("no route") },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run") },
                    closeChild = { o.close("child") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertTrue(failure.exceptionOrNull() is IllegalStateException)
        assertEquals(listOf("connect"), o.order, "nothing was acquired, so nothing is closed")
    }

    // ---- R: the F1 ownership hole ------------------------------------------

    @Test
    fun r8_aFailureAfterConnectButBeforeAttachStillClosesTheEndpoint() = ops { o ->
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe"); throw IllegalStateException("snapshot failed") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run") },
                    closeChild = { o.close("child") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertTrue(failure.exceptionOrNull() is IllegalStateException, "the failure reaches the caller")
        assertEquals(
            listOf("connect", "describe", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "the endpoint was acquired and then abandoned: a failure between connect and attach " +
                "leaves it open",
        )
    }

    @Test
    fun r9_aRefusedAttachStillClosesTheEndpoint() = ops { o ->
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); throw IllegalStateException("attach refused") },
                    run = { _, _ -> o.record("run") },
                    closeChild = { o.close("child") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertTrue(failure.exceptionOrNull() is IllegalStateException)
        assertEquals(
            listOf("connect", "describe", "attach", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "an endpoint whose attach was refused is still owned and must still be closed",
        )
    }

    @Test
    fun r10_aCancelledAttachStillClosesTheEndpoint() = ops { o ->
        val entered = CompletableDeferred<Unit>()
        val hold = CompletableDeferred<Unit>().also { o.barriers.add(it) }
        val job =
            launch {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = {
                        o.record("attach-begins")
                        entered.complete(Unit)
                        hold.await()
                        "child"
                    },
                    run = { _, _ -> o.record("run") },
                    closeChild = { o.close("child") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        testScheduler.advanceUntilIdle()
        assertTrue(entered.isCompleted, "the attach must be in flight")
        job.cancel()
        testScheduler.advanceUntilIdle()
        assertEquals(
            listOf("connect", "describe", "attach-begins", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "a session cancelled while attaching still owns the endpoint it connected",
        )
    }

    @Test
    fun r11_cleanupCompletesEvenWhenTheSessionIsAlreadyCancelled() = ops { o ->
        val entered = CompletableDeferred<Unit>()
        val job =
            launch {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ ->
                        entered.complete(Unit)
                        CompletableDeferred<Unit>().await()
                    },
                    closeChild = { o.close("child") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        testScheduler.advanceUntilIdle()
        assertTrue(entered.isCompleted)
        job.cancel()
        testScheduler.advanceUntilIdle()
        assertEquals(
            listOf("child:close-begins", "child:close-ends", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "each close must run to COMPLETION under cancellation; a close that suspends is " +
                "otherwise abandoned at its first suspension point, which is exactly what the " +
                "engine's ensureActive() does to a real close",
        )
    }

    @Test
    fun r12_aCleanupFailureNeitherHidesNorReplacesThePrimaryFailure() = ops { o ->
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> throw IllegalStateException("the body failed") },
                    closeChild = { o.record("child:close"); throw IllegalArgumentException("close failed") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        val thrown = failure.exceptionOrNull()
        assertTrue(
            thrown is IllegalStateException,
            "the BODY's failure is what the caller must see; a cleanup failure replaced it: " + thrown,
        )
        assertTrue(
            thrown!!.suppressed.any { it is IllegalArgumentException },
            "and the cleanup failure must still be reported, not swallowed; suppressed=" +
                thrown.suppressed.toList(),
        )
        assertEquals(
            listOf("child:close", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "a failing child close must not skip the endpoint close",
        )
    }

    @Test
    fun r13_cancellationRemainsCancellationEvenWhenCleanupAlsoFails() = ops { o ->
        var seenCancellation = false
        val entered = CompletableDeferred<Unit>()
        val job =
            launch {
                try {
                    broadcastLifetime<String, String>(
                        connect = { "endpoint" },
                        describe = { },
                        attach = { "child" },
                        run = { _, _ ->
                            entered.complete(Unit)
                            CompletableDeferred<Unit>().await()
                        },
                        closeChild = { throw IllegalStateException("close failed") },
                        closeEndpoint = { o.close("endpoint") },
                        )
                } catch (cancelled: CancellationException) {
                    seenCancellation = true
                    throw cancelled
                }
            }
        testScheduler.advanceUntilIdle()
        assertTrue(entered.isCompleted)
        job.cancel()
        testScheduler.advanceUntilIdle()
        assertTrue(
            seenCancellation,
            "a cancelled session whose cleanup also failed must still end as CANCELLED, not as " +
                "the cleanup's failure class",
        )
        assertEquals(
            listOf("endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "and the endpoint is still closed after the child close failed",
        )
    }

    // ---- promoted from 1748's private probes -------------------------------

    @Test
    fun r15_theEndpointIsStillClosedWhenTheChildCloseFailsAfterASuccessfulBody() = ops { o ->
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> },
                    closeChild = { o.record("child:close"); throw IllegalStateException("child close failed") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertEquals(
            listOf("child:close", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "a failing child close must not skip the endpoint close: every acquired resource is " +
                "closed even when an earlier cleanup fails",
        )
        val thrown = failure.exceptionOrNull()
        assertTrue(
            thrown is IllegalStateException && thrown.message == "child close failed",
            "with no primary failure, the FIRST cleanup failure is the outcome; got " + thrown,
        )
    }

    @Test
    fun r16_bothCleanupFailuresAreCarried() = ops { o ->
        val childFailure = IllegalStateException("child close failed")
        val endpointFailure = IllegalArgumentException("endpoint close failed")
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> },
                    closeChild = { o.record("child:close"); throw childFailure },
                    closeEndpoint = { o.record("endpoint:close"); throw endpointFailure },
                )
            }
        assertEquals(listOf("child:close", "endpoint:close"), o.order, "both closes are attempted")
        val thrown = failure.exceptionOrNull()
        assertTrue(
            thrown === childFailure,
            "the first cleanup failure INSTANCE is the outcome -- a same-class, same-message copy " +
                "is a different fact; got " + thrown,
        )
        assertEquals(
            listOf<Throwable>(endpointFailure),
            thrown!!.suppressed.toList(),
            "and exactly the endpoint's own instance is attached, in order, with no extras",
        )
    }

    @Test
    fun r17_theActualCancellationInstanceIsWhatPropagates() = ops { o ->
        val raised = CancellationException("the body's own cancellation")
        val childClose = IllegalStateException("child close failed")
        var seen: Throwable? = null
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> throw raised },
                    closeChild = { o.record("child:close"); throw childClose },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        seen = failure.exceptionOrNull()
        assertTrue(
            seen === raised,
            "the EXACT cancellation the body raised must propagate -- not a cleanup failure and " +
                "not a fresh cancellation; got " + seen,
        )
        assertEquals(
            listOf<Throwable>(childClose),
            seen!!.suppressed.toList(),
            "and exactly that cleanup failure INSTANCE is attached to it as owned evidence, not " +
                "routed to a UI message the delivery guard may discard",
        )
        assertEquals(
            listOf("child:close", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "with the endpoint still closed after the child close failed",
        )
    }

    @Test
    fun r18_aBodyFailureKeepsItsIdentityAndCarriesEveryCleanupFailureInOrder() = ops { o ->
        val body = IllegalStateException("the body failed")
        val childFailure = IllegalArgumentException("child close failed")
        val endpointFailure = UnsupportedOperationException("endpoint close failed")
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> throw body },
                    closeChild = { throw childFailure },
                    closeEndpoint = { throw endpointFailure },
                )
            }
        val thrown = failure.exceptionOrNull()
        assertTrue(thrown === body, "the body's own exception instance is the outcome; got " + thrown)
        assertEquals(
            listOf<Throwable>(childFailure, endpointFailure),
            thrown!!.suppressed.toList(),
            "both cleanup failure INSTANCES are attached, child first, with no extras",
        )
    }

    // ---- promoted from 1750's probes: a close raising cancellation ---------

    @Test
    fun r23_aChildCloseRaisingCancellationStillClosesTheEndpoint() = ops { o ->
        val raised = CancellationException("the child close cancelled")
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> },
                    closeChild = { o.record("child:close"); throw raised },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertEquals(
            listOf("child:close", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "NonCancellable stops INHERITED cancellation; it does not stop a close from raising " +
                "its own, and that must not abandon the next resource",
        )
        assertTrue(
            failure.exceptionOrNull() === raised,
            "with no body failure, that close's own cancellation is the outcome; got " +
                failure.exceptionOrNull(),
        )
    }

    @Test
    fun r24_aCloseCancellationDoesNotReplaceTheBodyFailure() = ops { o ->
        val body = IllegalStateException("the body failed")
        val closeCancel = CancellationException("the child close cancelled")
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> throw body },
                    closeChild = { o.record("child:close"); throw closeCancel },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertTrue(failure.exceptionOrNull() === body, "the BODY's failure is still the outcome")
        assertEquals(
            listOf(closeCancel),
            failure.exceptionOrNull()!!.suppressed.toList(),
            "and the close's cancellation is attached to it, by reference, with nothing else",
        )
        assertEquals(
            listOf("child:close", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "with the endpoint still closed",
        )
    }

    @Test
    fun r25_aCloseCancellationDoesNotReplaceTheBodyCancellation() = ops { o ->
        val bodyCancel = CancellationException("the body's own cancellation")
        val closeCancel = CancellationException("the child close cancelled")
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> throw bodyCancel },
                    closeChild = { throw closeCancel },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertTrue(
            failure.exceptionOrNull() === bodyCancel,
            "the session's OWN cancellation is what propagates, not the close's; got " +
                failure.exceptionOrNull(),
        )
        assertEquals(
            listOf(closeCancel),
            failure.exceptionOrNull()!!.suppressed.toList(),
            "with the close's cancellation attached by reference",
        )
    }

    @Test
    fun r26_anEndpointCloseCancellationKeepsTheFirstCleanupFailure() = ops { o ->
        val childFailure = IllegalStateException("child close failed")
        val endpointCancel = CancellationException("endpoint close cancelled")
        val failure =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { "endpoint" },
                    describe = { },
                    attach = { "child" },
                    run = { _, _ -> },
                    closeChild = { throw childFailure },
                    closeEndpoint = { throw endpointCancel },
                )
            }
        assertTrue(
            failure.exceptionOrNull() === childFailure,
            "the FIRST cleanup failure stays the outcome; got " + failure.exceptionOrNull(),
        )
        assertEquals(
            listOf(endpointCancel),
            failure.exceptionOrNull()!!.suppressed.toList(),
            "and the later close cancellation is attached to it rather than replacing it",
        )
    }

    // ---- E: the cleanup must survive an Error on either side ----------------

    /** An Error out of the body is the outcome, and it carries what the cleanup found. */
    @Test
    fun e1_anErrorFromTheBodyKeepsItsIdentityAndCarriesACleanupFailure() = ops { o ->
        val raised = AssertionError("the body failed with an Error")
        val closeFailure = IOException("the sender's close failed")
        val outcome =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run"); throw raised },
                    closeChild = { o.record("child:close"); throw closeFailure },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertSame(raised, outcome.exceptionOrNull(), "the body's Error is the outcome, by identity")
        assertTrue(raised.suppressed.any { it === closeFailure }, "carrying the cleanup failure it found")
        assertEquals(
            listOf("connect", "describe", "attach", "run", "child:close", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "and the endpoint was closed after the child close that failed",
        )
    }

    /** An Error out of the CHILD close does not abandon the endpoint behind it. */
    @Test
    fun e2_anErrorFromTheChildCloseStillClosesTheEndpoint() = ops { o ->
        val raised = AssertionError("the sender's close failed with an Error")
        val outcome =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run") },
                    closeChild = { o.record("child:close"); throw raised },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertSame(raised, outcome.exceptionOrNull(), "with no body primary, that Error is the outcome")
        assertEquals(
            listOf("connect", "describe", "attach", "run", "child:close", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
            "every acquired resource is still attempted, in order",
        )
    }

    /** The same for the later close. */
    @Test
    fun e3_anErrorFromTheEndpointCloseIsTheOutcome() = ops { o ->
        val raised = AssertionError("the endpoint's close failed with an Error")
        val outcome =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run") },
                    closeChild = { o.close("child") },
                    closeEndpoint = { o.record("endpoint:close"); throw raised },
                )
            }
        assertSame(raised, outcome.exceptionOrNull())
        assertEquals(
            listOf("connect", "describe", "attach", "run", "child:close-begins", "child:close-ends", "endpoint:close"),
            o.order,
        )
    }

    /** Mixed domains with no body primary: the FIRST cleanup failure carries the rest. */
    @Test
    fun e4_mixedCleanupDomainsKeepTheFirstFailureAsTheCarrier() = ops { o ->
        val first = CancellationException("the child's close cancelled itself")
        val later = AssertionError("the endpoint's close failed with an Error")
        val outcome =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run") },
                    closeChild = { o.record("child:close"); throw first },
                    closeEndpoint = { o.record("endpoint:close"); throw later },
                )
            }
        assertSame(first, outcome.exceptionOrNull(), "the first cleanup failure is the outcome")
        assertTrue(first.suppressed.any { it === later }, "carrying the later one")
        assertTrue(first.suppressed.none { it === first }, "and never itself")
        assertEquals(listOf("connect", "describe", "attach", "run", "child:close", "endpoint:close"), o.order)
    }

    /** A body primary carries every distinct cleanup failure, in order, once each. */
    @Test
    fun e5_aBodyPrimaryCarriesEveryDistinctCleanupFailure() = ops { o ->
        val primary = IllegalStateException("the broadcast failed")
        val childFailure = IOException("the sender's close failed")
        val endpointFailure = AssertionError("the endpoint's close failed with an Error")
        val outcome =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run"); throw primary },
                    closeChild = { o.record("child:close"); throw childFailure },
                    closeEndpoint = { o.record("endpoint:close"); throw endpointFailure },
                )
            }
        assertSame(primary, outcome.exceptionOrNull(), "the actual primary")
        assertEquals(
            listOf<Throwable>(childFailure, endpointFailure),
            primary.suppressed.toList(),
            "carrying both cleanup failures, in the order they happened, once each",
        )
        assertEquals(listOf("connect", "describe", "attach", "run", "child:close", "endpoint:close"), o.order)
    }

    /** The control: an Error with clean closes leaves alone, carrying nothing. */
    @Test
    fun e6_anErrorWithCleanClosesCarriesNothing() = ops { o ->
        val raised = AssertionError("the body failed with an Error")
        val outcome =
            runCatching {
                broadcastLifetime<String, String>(
                    connect = { o.record("connect"); "endpoint" },
                    describe = { o.record("describe") },
                    attach = { o.record("attach"); "child" },
                    run = { _, _ -> o.record("run"); throw raised },
                    closeChild = { o.close("child") },
                    closeEndpoint = { o.close("endpoint") },
                )
            }
        assertSame(raised, outcome.exceptionOrNull())
        assertEquals(emptyList(), raised.suppressed.toList(), "nothing failed, so nothing is attached")
        assertEquals(
            listOf("connect", "describe", "attach", "run", "child:close-begins", "child:close-ends", "endpoint:close-begins", "endpoint:close-ends"),
            o.order,
        )
    }
}
