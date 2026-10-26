package org.openmoq.moq5

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BackendWaitResult
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.StopRefusedException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The public endpoint LIFECYCLE surface (chats/libmoq/1388), RED-first, over
 * the scripted engine backend so every row is deterministic.
 *
 * What is re-derived here, from the service contract rather than the old
 * sketch:
 *
 *  * `snapshots: StateFlow<EndpointSnapshot>` -- ONE coherent record per
 *    endpoint activity. The service marks wait-activity whenever a pump cycle
 *    changes state, so the engine can take the record ON ITS WORKER each time
 *    its wait returns ACTIVITY and publish it. No timer, no separate native
 *    reads composed into a state.
 *  * `awaitEstablished(timeout)` -- suspends on that same record stream;
 *    resolves on ESTABLISHED, throws the typed terminal on a terminal record,
 *    throws the typed timeout when the caller's bound elapses. The bound is the
 *    caller's coroutine timeout, never a wall-clock poll of the endpoint.
 *  * `close()` -- non-suspending: rejects new operations at once and schedules
 *    the teardown on the engine; `closeAndJoin()` remains the deterministic
 *    boundary; `withMoqEndpoint` guarantees it.
 *  * cancellation is the CALLER's: cancelling a suspended wait cancels that
 *    wait only. The native interrupt latch is a different thing and is never
 *    set or cleared by cancellation.
 *
 * Deferred by source (see the report): `interrupt()` / `resume()` need the
 * `moq_endpoint_set_interrupted` row and `drain(timeout)` the
 * `moq_endpoint_drain` row, neither of which the JNI authority carries; and
 * drain must run OFF the service thread by the C contract, which the token's
 * authority-thread confinement forbids today.
 */
internal class PublicEndpointLifecycleTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"

        /** A bound on how long a deterministic row waits for the engine. Never an oracle. */
        val BOUND = 5.seconds

        val ESTABLISHED =
            EndpointSnapshot(EndpointState.Established, MoqVersion(0xff000012u), null)
        val CLOSED_TRANSPORT =
            EndpointSnapshot(
                EndpointState.Closed,
                MoqVersion(0xff000012u),
                EndpointTerminal.Transport(EndpointErrorDetail(0x8000_0000_0000_0001uL)),
            )
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    // -- snapshots: one record per activity, published from the worker -------

    @Test
    fun `snapshots starts from the record taken on the worker at construction`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertEquals(EndpointState.Connecting, endpoint.snapshots.value.state)
            assertEquals(1, backend.observed().callOrder.count { it == "snapshot" },
                "exactly one native record was taken to seed the flow")
            endpoint.closeAndJoin()
        }

    @Test
    fun `an endpoint activity republishes one coherent record without any caller call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED)
            backend.markActivity() // the service's coalesced, level-retained activity
            val seen = withTimeout(BOUND) { endpoint.snapshots.first { it.state == EndpointState.Established } }
            assertEquals(ESTABLISHED, seen, "the published record is the one native record, whole")
            // Event-driven: the record count follows the activity count, not a clock.
            val snaps = backend.observed().callOrder.count { it == "snapshot" }
            val activities = backend.observed().waitReturns.count { it == BackendWaitResult.ACTIVITY }
            assertEquals(1 + activities, snaps, "one seed record plus one per activity; no polling")
            endpoint.closeAndJoin()
        }

    // -- awaitEstablished ------------------------------------------------------

    @Test
    fun `awaitEstablished resolves from endpoint activity and takes no extra native reads`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val waiter = async(start = CoroutineStart.UNDISPATCHED) { endpoint.awaitEstablished(BOUND) }
            val before = backend.observed().callOrder.count { it == "snapshot" }
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED)
            backend.markActivity()
            withTimeout(BOUND) { waiter.await() }
            val after = backend.observed().callOrder.count { it == "snapshot" }
            assertEquals(1, after - before, "exactly the one record the activity earned; nothing was polled")
            endpoint.closeAndJoin()
        }

    @Test
    fun `awaitEstablished times out by the caller's bound and never polls the endpoint`() =
        kotlinx.coroutines.test.runTest {
            // VIRTUAL time: the caller's bound elapses on the test scheduler
            // the moment nothing else can run, so no wall clock is waited on.
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val before = backend.observed().callOrder.count { it == "snapshot" }
            val failure = assertFailsWith<EndpointTimeoutException> { endpoint.awaitEstablished(200.milliseconds) }
            assertEquals(200.milliseconds, failure.timeout, "the failure names the bound that elapsed")
            assertEquals(before, backend.observed().callOrder.count { it == "snapshot" },
                "no activity, no record: a timeout must not be discovered by re-reading the endpoint")
            assertTrue(backend.observed().waitReturns.none { it == BackendWaitResult.INTERRUPTED },
                "a caller timeout is not a native interrupt")
            endpoint.closeAndJoin()
        }

    @Test
    fun `awaitEstablished on a terminal record throws the typed terminal, not a timeout`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val waiter = async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { endpoint.awaitEstablished(BOUND) }.exceptionOrNull()
            }
            backend.snapshotResult = BackendSnapshotResult.Ok(CLOSED_TRANSPORT)
            backend.markActivity()
            val thrown = withTimeout(BOUND) { waiter.await() }
            assertTrue(thrown is EndpointClosedException, "a terminal record must surface as closed; saw $thrown")
            assertEquals(CLOSED_TRANSPORT.terminal, (thrown as EndpointClosedException).terminal,
                "the exact terminal, in its own domain, travels with the failure")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a bridge refusal on the background activity record quarantines exactly like an explicit snapshot`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val waiter = async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { endpoint.awaitEstablished(BOUND) }.exceptionOrNull()
            }
            // The next activity read answers a private-registry refusal.
            backend.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", -1002))
            backend.markActivity()
            val thrown = withTimeout(BOUND) { waiter.await() }
            assertTrue(thrown is BridgeContractException, "the waiter must learn the bridge refusal; saw $thrown")
            assertEquals(-1002, (thrown as BridgeContractException).failure.code, "the exact private code")
            // Quarantined: no further backend call, the join reports the same
            // stable failure, and nothing is destroyed.
            val joined = runCatching { endpoint.closeAndJoin() }.exceptionOrNull()
            assertTrue(joined is BridgeContractException, "the join must report the quarantine; saw $joined")
            assertEquals(0, backend.observed().stopCount, "a quarantined root is not stopped")
            assertEquals(0, backend.observed().destroyCount, "a quarantined root is not destroyed")
        }

    @Test
    fun `a bridge refusal on the SEED record fails open with the exact bridge failure and quarantines`() =
        runTest {
            val backend = newBackend()
            backend.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", -1002))
            val thrown = runCatching { MoqEndpoint.open(EndpointConfig(URL)) { backend } }.exceptionOrNull()
            assertTrue(thrown is BridgeContractException,
                "open() must surface the seed's bridge refusal, never a stream-less endpoint; saw $thrown")
            assertEquals(-1002, (thrown as BridgeContractException).failure.code, "the exact private code")
            // The bridge refused the token: nothing may be stopped or destroyed,
            // and the worker must retire rather than park forever.
            assertEquals(0, backend.observed().stopCount)
            assertEquals(0, backend.observed().destroyCount)
            val worker = backend.observed().serviceThread
            assertTrue(worker != null, "the worker must have run the factory")
            worker!!.join(BOUND.inWholeMilliseconds)
            assertTrue(!worker.isAlive, "the worker must retire after a quarantined construction")
        }

    @Test
    fun `a terminal CLOSED wait result yields exactly one terminal record and closes a pending waiter`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val waiter = async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { endpoint.awaitEstablished(BOUND) }.exceptionOrNull()
            }
            backend.awaitCondition("the wait is parked") { it.waitParked }
            val before = backend.observed().callOrder.count { it == "snapshot" }
            // The documented moq_endpoint_wait() answer once terminal, through
            // the WAIT path -- not an activity.
            backend.snapshotResult = BackendSnapshotResult.Ok(CLOSED_TRANSPORT)
            backend.setClosed(true)
            val thrown = withTimeout(BOUND) { waiter.await() }
            assertTrue(thrown is EndpointClosedException,
                "a terminal wait result must close the waiter with the terminal, not time it out; saw $thrown")
            assertEquals(CLOSED_TRANSPORT.terminal, (thrown as EndpointClosedException).terminal)
            assertEquals(1, backend.observed().callOrder.count { it == "snapshot" } - before,
                "exactly one record for the terminal wait result")
            assertEquals(CLOSED_TRANSPORT, endpoint.snapshots.value, "and it is the published record")
            endpoint.closeAndJoin()
        }

    // -- cancellation is the caller's; the native latch is untouched ---------

    @Test
    fun `cancelling a pending awaitEstablished cancels only that wait`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val waiter = launch(Dispatchers.Default) { endpoint.awaitEstablished(BOUND) }
            backend.awaitCondition("the wait is parked") { it.waitParked }
            waiter.cancel()
            withTimeout(BOUND) { waiter.join() }
            assertTrue(waiter.isCancelled, "the caller's wait is cancelled")
            // The endpoint itself was not interrupted: no INTERRUPTED answer,
            // and a later wait still resolves from activity.
            assertTrue(backend.observed().waitReturns.none { it == BackendWaitResult.INTERRUPTED })
            val again = async(start = CoroutineStart.UNDISPATCHED) { endpoint.awaitEstablished(BOUND) }
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED)
            backend.markActivity()
            withTimeout(BOUND) { again.await() }
            endpoint.closeAndJoin()
        }

    // -- close(): immediate; closeAndJoin(): deterministic ---------------------

    @Test
    fun `close is non-suspending, rejects new operations at once, and joins exactly once later`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.close() // a plain function: no suspension, no join
            assertFailsWith<EndpointClosedException> { endpoint.snapshot() }
            assertFailsWith<EndpointClosedException> { endpoint.awaitEstablished(BOUND) }
            endpoint.close() // idempotent
            endpoint.closeAndJoin()
            endpoint.closeAndJoin()
            assertEquals(1, backend.observed().stopCount, "one native stop")
            assertEquals(1, backend.observed().destroyCount, "one native destroy")
            assertTrue(backend.observed().callOrder.indexOf("stop") < backend.observed().callOrder.indexOf("destroy"))
        }

    @Test
    fun `closeAndJoin never implies a drain`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.closeAndJoin()
            assertTrue(backend.observed().callOrder.none { it.contains("drain") },
                "teardown is stop then destroy; a flush is a separate, fallible operation")
        }

    // -- withMoqEndpoint ------------------------------------------------------

    @Test
    fun `withMoqEndpoint returns the block's value and always joins the teardown`() =
        runTest {
            val backend = newBackend()
            val value = withMoqEndpoint(EndpointConfig(URL), { backend }) { endpoint ->
                endpoint.snapshot().state
            }
            assertEquals(EndpointState.Connecting, value)
            assertEquals(listOf("stop", "destroy"), backend.observed().callOrder.filter { it != "snapshot" })
            // The seed, the explicit snapshot, and one record per activity the
            // engine's own wakes caused; never fewer than the seed plus the read.
            assertTrue(backend.observed().callOrder.count { it == "snapshot" } >= 2)
        }

    @Test
    fun `withMoqEndpoint propagates the block's failure after joining the teardown`() =
        runTest {
            val backend = newBackend()
            val boom = IllegalStateException("app failure")
            val thrown = assertFailsWith<IllegalStateException> {
                withMoqEndpoint(EndpointConfig(URL), { backend }) { throw boom }
            }
            assertTrue(thrown === boom, "the block's own failure, not a teardown wrapper")
            assertEquals(1, backend.observed().stopCount)
            assertEquals(1, backend.observed().destroyCount)
        }

    @Test
    fun `withMoqEndpoint keeps the block's failure when the teardown itself fails, retaining the teardown failure as suppressed`() =
        runTest {
            val backend = newBackend()
            val boom = IllegalStateException("app failure")
            backend.refuseStopWith(MoqResultCode(-5)) // the teardown will be refused
            val thrown = runCatching {
                withMoqEndpoint(EndpointConfig(URL), { backend }) { throw boom }
            }.exceptionOrNull()
            assertTrue(thrown === boom, "the block's own failure must be the outcome; saw $thrown")
            val suppressed = boom.suppressed.toList()
            assertTrue(suppressed.any { it is StopRefusedException },
                "the teardown failure must be retained as suppressed evidence; saw $suppressed")
            assertEquals(MoqResultCode(-5), (suppressed.first { it is StopRefusedException } as StopRefusedException).code)
        }

    @Test
    fun `withMoqEndpoint surfaces a teardown failure when the block itself succeeded`() =
        runTest {
            val backend = newBackend()
            backend.refuseStopWith(MoqResultCode(-5))
            val thrown = runCatching {
                withMoqEndpoint(EndpointConfig(URL), { backend }) { "fine" }
            }.exceptionOrNull()
            assertTrue(thrown is StopRefusedException, "a refused teardown must not be swallowed; saw $thrown")
            assertEquals(MoqResultCode(-5), (thrown as StopRefusedException).code)
        }

    @Test
    fun `withMoqEndpoint completes the teardown even when the caller is cancelled inside the block`() =
        runTest {
            val backend = newBackend()
            val inBlock = gate("in-block")
            val job = launch(Dispatchers.Default) {
                withMoqEndpoint(EndpointConfig(URL), { backend }) {
                    inBlock.open()
                    kotlinx.coroutines.awaitCancellation()
                }
            }
            assertTrue(inBlock.await(), "the block never ran")
            job.cancel()
            withTimeout(BOUND) { job.join() }
            assertTrue(job.isCancelled)
            // Teardown is cancellation-immune: exactly once, in order.
            assertTrue(
                backend.awaitCondition("teardown completes") { it.destroyCount == 1 },
                "a cancelled caller must still get its endpoint torn down",
            )
            assertEquals(listOf("stop", "destroy"), backend.observed().callOrder.filter { it != "snapshot" })
        }

    @Suppress("unused")
    private fun quiet(e: CancellationException) = e
}
