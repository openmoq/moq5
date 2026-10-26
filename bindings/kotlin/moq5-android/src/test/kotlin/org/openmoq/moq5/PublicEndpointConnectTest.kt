package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EndpointCreateException
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.StopRefusedException
import java.lang.reflect.Modifier
import kotlin.coroutines.Continuation
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * `MoqEndpoint.connect(config, timeout)`, RED-first: the composition the
 * plan names (§3b) and the one thing `open()` + `awaitEstablished()` cannot
 * promise on their own -- that NO ENDPOINT ESCAPES a failed establishment.
 *
 * Derived contract: `connect` = `open` (an open failure propagates with
 * nothing to close, exactly as `withMoqEndpoint`), then `awaitEstablished
 * (timeout)` on the returned endpoint, and on ANY failure of that wait --
 * the typed timeout, the typed terminal, a bridge quarantine, or the
 * caller's cancellation -- `closeAndJoin()` under `NonCancellable` before the
 * failure is rethrown, with a failing teardown retained as suppressed on it
 * (the `withMoqEndpoint` discipline). The timeout is `awaitEstablished`'s:
 * the caller's coroutine bound, never a poll, with the same domain. No new
 * engine behaviour, no JNI row, no native read of its own.
 */
internal class PublicEndpointConnectTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
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

    @Test
    fun `connect is a suspending companion member of a config and a Duration`() {
        val companion = MoqEndpoint.Companion::class.java
        val declared = companion.declaredMethods.filter { it.name == "connect" || it.name.startsWith("connect-") }
        // Exactly one PUBLIC shape; the factory-taking overload is internal.
        val public = declared.filter { Modifier.isPublic(it.modifiers) && it.parameterTypes.size == 3 }
        assertEquals(1, public.size, "one public connect(config, timeout); found ${declared.map { it.toGenericString() }}")
        val m = public.single()
        assertEquals(listOf(EndpointConfig::class.java, java.lang.Long.TYPE, Continuation::class.java), m.parameterTypes.toList(),
            "a Duration lowers to long; suspend adds the Continuation")
        assertEquals(Any::class.java, m.returnType)
    }

    @Test
    fun `connect returns once the first record is Established, taking no extra native read`() =
        runTest {
            val backend = newBackend()
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED)
            val endpoint = withTimeout(BOUND) { MoqEndpoint.connect(EndpointConfig(URL), BOUND) { backend } }
            assertEquals(ESTABLISHED, endpoint.snapshots.value)
            assertEquals(1, backend.observed().callOrder.count { it == "snapshot" }, "the seed record alone decided it")
            endpoint.closeAndJoin()
        }

    @Test
    fun `connect resolves from endpoint activity that establishes after the seed`() =
        runTest {
            val backend = newBackend()
            val connecting = launch(Dispatchers.Default) {
                val endpoint = MoqEndpoint.connect(EndpointConfig(URL), BOUND) { backend }
                assertEquals(EndpointState.Established, endpoint.snapshots.value.state)
                endpoint.closeAndJoin()
            }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED)
            backend.markActivity()
            withTimeout(BOUND) { connecting.join() }
            assertEquals(1, backend.observed().stopCount)
            assertEquals(1, backend.observed().destroyCount)
        }

    @Test
    fun `connect tears down on the caller's timeout and throws the typed timeout, so no endpoint escapes`() =
        kotlinx.coroutines.test.runTest {
            // VIRTUAL time, exactly as the awaitEstablished timeout row.
            val backend = newBackend()
            val failure = assertFailsWith<EndpointTimeoutException> {
                MoqEndpoint.connect(EndpointConfig(URL), 200.milliseconds) { backend }
            }
            assertEquals(200.milliseconds, failure.timeout)
            // Torn down BEFORE the throw: stop then destroy, exactly once.
            assertEquals(listOf("stop", "destroy"), backend.observed().callOrder.filter { it == "stop" || it == "destroy" })
            assertTrue(backend.observed().latchSets.isEmpty(), "a caller timeout is not a native interrupt")
        }

    @Test
    fun `connect tears down on a terminal record and throws the typed terminal`() =
        runTest {
            val backend = newBackend()
            backend.snapshotResult = BackendSnapshotResult.Ok(CLOSED_TRANSPORT)
            val failure = assertFailsWith<EndpointClosedException> {
                MoqEndpoint.connect(EndpointConfig(URL), BOUND) { backend }
            }
            assertEquals(CLOSED_TRANSPORT.terminal, failure.terminal, "the exact terminal from the record")
            assertEquals(1, backend.observed().stopCount)
            assertEquals(1, backend.observed().destroyCount)
        }

    @Test
    fun `connect keeps the establishment failure when its teardown fails, retaining the teardown failure as suppressed`() =
        runTest {
            val backend = newBackend()
            backend.snapshotResult = BackendSnapshotResult.Ok(CLOSED_TRANSPORT)
            backend.refuseStopWith(MoqResultCode(-5))
            val failure = assertFailsWith<EndpointClosedException> {
                MoqEndpoint.connect(EndpointConfig(URL), BOUND) { backend }
            }
            assertEquals(CLOSED_TRANSPORT.terminal, failure.terminal)
            val suppressed = failure.suppressed.toList()
            assertTrue(suppressed.any { it is StopRefusedException && it.code == MoqResultCode(-5) },
                "the refused teardown is evidence, never the outcome; saw $suppressed")
            assertEquals(0, backend.observed().destroyCount, "a refused stop quarantines: no destroy")
        }

    @Test
    fun `an open failure propagates from connect with nothing to close`() =
        runTest {
            val thrown = assertFailsWith<EndpointCreateFailedException> {
                MoqEndpoint.connect(EndpointConfig(URL), BOUND) { throw EndpointCreateException(-2) }
            }
            assertEquals(MoqResultCode(-2), thrown.endpointCode)
        }

    @Test
    fun `a bridge refusal on the seed record fails connect with the exact bridge failure and no teardown call`() =
        runTest {
            val backend = newBackend()
            backend.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", -1002))
            val thrown = assertFailsWith<BridgeContractException> {
                MoqEndpoint.connect(EndpointConfig(URL), BOUND) { backend }
            }
            assertEquals(-1002, thrown.failure.code)
            assertEquals(0, backend.observed().stopCount)
            assertEquals(0, backend.observed().destroyCount)
        }

    @Test
    fun `a bridge refusal on a post-open activity record fails connect with the exact bridge failure, no teardown call, and no self-suppression`() =
        runTest {
            val backend = newBackend()
            var worker: Thread? = null
            val connecting = async(Dispatchers.Default) {
                runCatching {
                    MoqEndpoint.connect(EndpointConfig(URL), BOUND) {
                        worker = Thread.currentThread()
                        backend
                    }
                }
            }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            // The seed was Connecting; the NEXT record is refused by the bridge.
            backend.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", -1002))
            backend.markActivity()
            val thrown = withTimeout(BOUND) { connecting.await() }.exceptionOrNull()
            assertTrue(thrown is BridgeContractException,
                "the exact quarantine failure, never a self-suppression IllegalArgumentException; saw $thrown")
            assertEquals(-1002, (thrown as BridgeContractException).failure.code)
            assertTrue(thrown.suppressed.none { it === thrown }, "no self-suppressed copy of the failure")
            // The quarantine is the engine's: the cleanup reports the SAME
            // stable failure, and the token is not teardown-owned.
            assertEquals(0, backend.observed().stopCount)
            assertEquals(0, backend.observed().destroyCount)
            assertTrue(worker != null, "the worker ran the factory")
            worker!!.join(BOUND.inWholeMilliseconds)
            assertTrue(!worker!!.isAlive, "the worker retires after the cleanup rather than parking forever")
        }

    @Test
    fun `cancelling connect while establishing tears down and rethrows the cancellation`() =
        runTest {
            val backend = newBackend()
            val job = launch(Dispatchers.Default) { MoqEndpoint.connect(EndpointConfig(URL), BOUND) { backend } }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            job.cancel()
            withTimeout(BOUND) { job.join() }
            assertTrue(job.isCancelled)
            assertTrue(backend.awaitCondition("teardown completes") { it.destroyCount == 1 })
            assertEquals(listOf("stop", "destroy"), backend.observed().callOrder.filter { it == "stop" || it == "destroy" })
            assertTrue(backend.observed().latchSets.isEmpty(), "cancellation never touches the latch")
        }
}
