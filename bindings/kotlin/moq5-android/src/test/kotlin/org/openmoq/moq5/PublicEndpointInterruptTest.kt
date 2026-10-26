package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendOpResult
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BackendWaitResult
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The public interrupt latch, RED-first, over the scripted engine backend.
 *
 * Re-derived from `moq_endpoint_set_interrupted` (endpoint.h §5.3): sticky,
 * non-terminal, idempotent, callable from any thread; while latched every
 * blocking endpoint call answers MOQ_ERR_INTERRUPTED at once, and clearing it
 * restores normal blocking. On this topology that means:
 *
 *  * `interrupt()` sets the native latch ON THE CALLER'S THREAD, at once --
 *    never marshalled to the worker, which is the thread the latch exists to
 *    unblock. The C call wakes the facade itself, so no engine wake is sent.
 *  * the worker's wait then answers INTERRUPTED and the worker parks on the
 *    engine's own condition (the latch would make every further wait return
 *    immediately). No record is published: the latch is not a state change.
 *  * `resume()` clears the native latch and wakes the ENGINE out of that park,
 *    so the worker re-enters the backend wait now rather than when unrelated
 *    work happens to arrive. No backend wake is needed for that either.
 *  * a pending `awaitEstablished()` is a suspended observer, not a blocking
 *    endpoint call: the latch neither fails nor resolves it. Cancellation
 *    stays the coroutine's own mechanism and never touches the latch.
 *  * after a logical close both calls refuse with `EndpointClosedException`,
 *    like every other public operation; after a bridge quarantine both report
 *    the same stable `BridgeContractException` and make no native call. A
 *    bridge refusal of the latch row itself is a quarantine and never a
 *    `MoqResultCode`.
 */
internal class PublicEndpointInterruptTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"

        /** A ceiling on how long a deterministic row waits for the engine. Never an oracle. */
        val BOUND = 5.seconds

        val ESTABLISHED =
            EndpointSnapshot(EndpointState.Established, MoqVersion(0xff000012u), null)
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    @Test
    fun `interrupt sets the native latch at once on the caller's thread and the parked wait answers INTERRUPTED`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            val wakesBefore = backend.observed().wakeCount
            val recordsBefore = backend.observed().callOrder.count { it == "snapshot" }

            endpoint.interrupt()

            // Set on THIS thread, synchronously: the call has returned and the
            // latch write is already recorded, attributed to the caller.
            assertEquals(listOf(true), backend.observed().latchSets)
            assertEquals(Thread.currentThread().threadId(), backend.observed().latchThreadIds.single())
            assertTrue(
                backend.awaitCondition("the parked wait answers INTERRUPTED") {
                    it.waitReturns.lastOrNull() == BackendWaitResult.INTERRUPTED
                },
                "the latch must break the worker's wait; saw ${backend.observed().waitReturns}",
            )
            // Not marshalled: no job was queued, so no engine wake reached the
            // backend, and the latch is not a state change, so no record.
            assertEquals(wakesBefore, backend.observed().wakeCount, "interrupt() must not submit work")
            assertEquals(recordsBefore, backend.observed().callOrder.count { it == "snapshot" })
            assertEquals(EndpointState.Connecting, endpoint.snapshots.value.state)
            endpoint.resume()
            endpoint.closeAndJoin()
        }

    @Test
    fun `resume wakes the worker out of its park so the cleared latch is visible without unrelated work`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            endpoint.interrupt()
            assertTrue(
                backend.awaitCondition("INTERRUPTED observed") {
                    it.waitReturns.lastOrNull() == BackendWaitResult.INTERRUPTED
                },
            )
            // The worker is parked on the engine, not spinning on the backend:
            // the wait entry count is stable while the latch stands.
            val entries = backend.observed().waitEntries
            assertTrue(
                !backend.awaitCondition("no re-entry while latched", ceilingMillis = 200) {
                    it.waitEntries != entries
                },
                "a latched backend must not be re-entered until something wakes the engine",
            )
            val wakesBefore = backend.observed().wakeCount

            endpoint.resume()

            assertEquals(listOf(true, false), backend.observed().latchSets)
            // The engine itself woke the worker: exactly one fresh wait entry,
            // parked again in the backend, with no job and no backend wake.
            assertTrue(
                backend.awaitCondition("the worker re-enters the backend wait") {
                    it.waitEntries == entries + 1 && it.waitParked
                },
                "resume() must wake the engine out of its park; entries=${backend.observed().waitEntries}",
            )
            assertEquals(wakesBefore, backend.observed().wakeCount, "resume() needs no backend wake")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a latched wait neither fails nor resolves a pending awaitEstablished, which resolves from activity after resume`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val waiter = async(Dispatchers.Default) { runCatching { endpoint.awaitEstablished(BOUND) } }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })

            endpoint.interrupt()
            assertTrue(
                backend.awaitCondition("INTERRUPTED observed") {
                    it.waitReturns.lastOrNull() == BackendWaitResult.INTERRUPTED
                },
            )
            assertTrue(waiter.isActive, "the latch is not a terminal and not a timeout: the waiter stays pending")

            endpoint.resume()
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED)
            backend.markActivity()
            val outcome = withTimeout(BOUND) { waiter.await() }
            assertTrue(outcome.isSuccess, "ordinary activity after resume must establish; saw $outcome")
            assertEquals(ESTABLISHED, endpoint.snapshots.value)
            endpoint.closeAndJoin()
        }

    @Test
    fun `interrupt and resume are plain idempotent functions`() =
        runTest {
            for (name in listOf("interrupt", "resume")) {
                val m = MoqEndpoint::class.java.getMethod(name)
                assertEquals(0, m.parameterCount, "$name() takes no continuation: it never suspends")
                assertEquals(Void.TYPE, m.returnType)
                assertTrue(Modifier.isPublic(m.modifiers))
            }
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.interrupt()
            endpoint.interrupt()
            endpoint.resume()
            endpoint.resume()
            // Idempotent at the C contract: each call is passed through as the
            // same store, never coalesced away and never turned into a toggle.
            assertEquals(listOf(true, true, false, false), backend.observed().latchSets)
            endpoint.closeAndJoin()
        }

    @Test
    fun `cancelling a pending awaitEstablished never sets or clears the latch`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val waiter = launch(Dispatchers.Default) { endpoint.awaitEstablished(BOUND) }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            waiter.cancel()
            withTimeout(BOUND) { waiter.join() }
            assertTrue(waiter.isCancelled)
            assertTrue(backend.observed().latchSets.isEmpty(), "cancellation is the coroutine's; the latch is the app's")
            assertTrue(backend.observed().waitReturns.none { it == BackendWaitResult.INTERRUPTED })
            endpoint.closeAndJoin()
        }

    // -- failure shape ---------------------------------------------------------

    @Test
    fun `after close, interrupt and resume refuse with EndpointClosedException and make no native call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.close()
            assertFailsWith<EndpointClosedException> { endpoint.interrupt() }
            assertFailsWith<EndpointClosedException> { endpoint.resume() }
            endpoint.closeAndJoin()
            assertFailsWith<EndpointClosedException> { endpoint.interrupt() }
            assertTrue(backend.observed().latchSets.isEmpty(), "no latch call may reach a closing or retired root")
        }

    @Test
    fun `after a bridge quarantine, interrupt and resume report the stable failure and make no native call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", -1002))
            val first = assertFailsWith<BridgeContractException> { endpoint.snapshot() }
            val second = assertFailsWith<BridgeContractException> { endpoint.interrupt() }
            val third = assertFailsWith<BridgeContractException> { endpoint.resume() }
            assertEquals(first.failure, second.failure)
            assertEquals(first.failure, third.failure)
            assertTrue(backend.observed().latchSets.isEmpty(), "a quarantined token gets no further backend call")
            val late = runCatching { endpoint.closeAndJoin() }.exceptionOrNull()
            assertTrue(late is BridgeContractException && late.failure == first.failure)
            assertEquals(0, backend.observed().stopCount)
            assertEquals(0, backend.observed().destroyCount)
        }

    @Test
    fun `a bridge refusal of the latch row quarantines and is never a MoqResultCode`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.setInterruptedResult = BackendOpResult.Bridge(BridgeFailure("set_interrupted", -1002))
            val thrown = assertFailsWith<BridgeContractException> { endpoint.interrupt() }
            assertEquals(BridgeFailure("set_interrupted", -1002), thrown.failure, "the exact private code, on the row that refused")
            assertTrue(
                generateSequence(thrown::class.java as Class<*>) { it.superclass }
                    .flatMap { it.declaredFields.asSequence() }
                    .none { it.type == MoqResultCode::class.java },
                "a private bridge code must never be carried as a MoqResultCode",
            )
            // The quarantine is the engine's: every later operation reports the
            // same failure, and the teardown makes no backend call.
            val again = assertFailsWith<BridgeContractException> { endpoint.snapshot() }
            assertEquals(thrown.failure, again.failure)
            // The quarantine forbids the nudge that would end the worker's
            // current wait slice, so the worker learns of it when that slice
            // expires -- modelled here, as the C tier's bounded wait would.
            backend.expireWait()
            val late = runCatching { endpoint.closeAndJoin() }.exceptionOrNull()
            assertTrue(late is BridgeContractException && late.failure == thrown.failure)
            assertEquals(0, backend.observed().stopCount)
            assertEquals(0, backend.observed().destroyCount)
        }
}
