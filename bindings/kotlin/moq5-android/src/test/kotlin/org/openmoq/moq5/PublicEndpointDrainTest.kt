package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.openmoq.moq5.engine.BackendDrainResult
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BackendWaitResult
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import java.lang.reflect.Modifier
import kotlin.coroutines.Continuation
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.openmoq.moq5.engine.JniEndpointBackend
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Public graceful drain, RED-first, over the scripted engine backend.
 *
 * Re-derived from `moq_endpoint_drain()` (endpoint.h, endpoint.c): an
 * APP-THREAD BLOCKING call that loops -- hook status, transport probe, then
 * `moq_endpoint_wait()` in bounded slices -- until flushed, timed out,
 * interrupted (the latch is checked first and on every iteration), closed, or
 * refused as unsupported / wrong-thread. Two facts fix the Android shape:
 *
 *  * the drain WAITS ON THE ENDPOINT ITSELF, and a managed backend's wait
 *    consumes a single activity level (`activity_pending = false`). The engine
 *    worker is the endpoint's wait consumer, so a drain and the worker must
 *    not both sit in the native wait: the drain would consume the activity the
 *    record stream is built from. While a drain is in flight the worker is
 *    therefore QUIESCED from the native wait -- parked on the engine's own
 *    condition, still servicing jobs and close -- and takes ONE record when
 *    the drain returns, the coalesced observation the C tier itself keeps.
 *  * the C contract is any-thread for the application and blocking, so the
 *    row is LEASED like wake and set_interrupted and runs OFF THE SERVICE
 *    WORKER on a blocking dispatcher, never on the worker (which would stall
 *    every other public operation for up to the timeout) and never on the
 *    managed context (the C tier refuses that with WRONG_STATE).
 *  * the quiesce is ORDERED: a drain request first makes the worker leave the
 *    backend wait it is in and park on the engine's condition; only then does
 *    the native drain begin. And native drains are SERIALISED: nothing in the
 *    C contract proves two simultaneous callers safe, so at most one native
 *    drain is in flight, later callers suspend until it completes (or are
 *    cancelled before they start, making no native call).
 *  * the timeout is validated BEFORE any native call: negative or infinite
 *    durations never become a huge uint64 on the C side; ZERO is the C
 *    zero-timeout probe.
 *
 * `Closed` means the requested flush did NOT complete: the endpoint (or an
 * attached producer holding accepted output) terminalised first and queued
 * send data may have been truncated. It is a value because it is an ordinary
 * drain outcome, never a graceful completion.
 *
 * Result shape: a sealed value. Every non-flushed answer is an ordinary
 * answer to "did the flush finish in time" that the caller weighs before
 * stopping; none is a failure of the endpoint. That differs from
 * `awaitEstablished()`, whose timeout and terminal are failures of the goal
 * itself. Public-operation discipline is unchanged: after a logical close or a
 * bridge quarantine the call refuses like every other, with no native call,
 * and a bridge refusal of the drain row itself quarantines.
 */
internal class PublicEndpointDrainTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"

        /** A ceiling on how long a deterministic row waits. Never an oracle. */
        val BOUND = 5.seconds
        val TIMEOUT = 250.milliseconds

        val ESTABLISHED =
            EndpointSnapshot(EndpointState.Established, MoqVersion(0xff000012u), null)
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    // -- shape ------------------------------------------------------------------

    @Test
    fun `drain is a suspending member function of a Duration returning the sealed outcome`() {
        // The Kotlin source API: `endpoint.drain(Duration.ZERO)` is an ordinary
        // member call (the call below type-checks; it is never invoked).
        @Suppress("UNUSED_VARIABLE", "unused")
        val call: suspend (MoqEndpoint) -> DrainOutcome = { it.drain(Duration.ZERO) }
        // The JVM shape without overclaiming it: a value-class parameter
        // compiles to `long` under a possibly mangled name, and a suspend
        // function returns Object with a trailing Continuation. Declared ON
        // MoqEndpoint -- an extension-only implementation would live in a *Kt
        // file as a static method and is insufficient.
        val declared = MoqEndpoint::class.java.declaredMethods
            .filter { it.name == "drain" || it.name.startsWith("drain-") }
        assertEquals(1, declared.size, "exactly one drain member on MoqEndpoint; found ${declared.map { it.name }}")
        val m = declared.single()
        assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && !Modifier.isStatic(m.modifiers))
        assertEquals(listOf(java.lang.Long.TYPE, Continuation::class.java), m.parameterTypes.toList())
        assertEquals(Any::class.java, m.returnType)
        val outcomes = DrainOutcome::class.java.declaredClasses.map { it.simpleName }.toSet()
        assertEquals(setOf("Flushed", "TimedOut", "Interrupted", "Closed", "Unsupported"), outcomes,
            "exactly the C tier's answers, no invented state")
    }

    // -- thread / authority: the discriminator -------------------------------

    @Test
    fun `a drain request first makes the worker leave its backend wait and park, and only then does the native drain begin off the worker`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            // The worker is INSIDE a backend wait when the drain is requested.
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            backend.holdDrain() // the native call blocks until released
            val drain = async(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(backend.awaitCondition("the drain is in flight") { it.drainInFlight })
            // ORDER: recorded by the backend at the instant drain() was entered
            // -- no wait may still be parked then. The worker left its wait
            // (the wait returned) before the native drain began.
            assertTrue(!backend.observed().drainEnteredWhileWaitParked,
                "the native drain must not begin while the worker is inside a backend wait")
            val service = backend.observed().serviceThreadId
            assertTrue(backend.observed().drainThreadIds.single() != service, "the drain never runs on the worker")
            assertEquals(TIMEOUT.inWholeMicroseconds, backend.observed().drainTimeouts.single())
            // While the native drain is active the worker enters no backend
            // wait ...
            val entries = backend.observed().waitEntries
            assertTrue(!backend.observed().waitParked)
            // ... but still services ordinary engine jobs.
            val record = withTimeout(BOUND) { endpoint.snapshot() }
            assertEquals(EndpointState.Connecting, record.state)
            assertEquals(entries, backend.observed().waitEntries, "no backend wait while a native drain is active")
            backend.drainResult = BackendDrainResult.Flushed
            backend.releaseDrain()
            assertEquals(DrainOutcome.Flushed, withTimeout(BOUND) { drain.await() })
            // The worker returns to the backend wait once the native drain
            // released (the level the job's wake retained is consumed first).
            assertTrue(backend.awaitCondition("the worker re-enters the wait") { it.waitEntries > entries && it.waitParked })
            endpoint.closeAndJoin()
        }

    @Test
    fun `activity during a drain is not consumed by the worker and is observed after the drain returns`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            backend.holdDrain()
            val drain = async(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(backend.awaitCondition("the worker leaves the wait") { it.drainInFlight && !it.waitParked })
            val activityReturns = backend.observed().waitReturns.count { it == BackendWaitResult.ACTIVITY }
            val records = backend.observed().callOrder.count { it == "snapshot" }
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED)
            backend.markActivity()
            assertTrue(
                !backend.awaitCondition("no consumption while in flight", ceilingMillis = 200) {
                    it.waitReturns.count { r -> r == BackendWaitResult.ACTIVITY } != activityReturns
                },
                "the worker must not consume the level while the drain owns the endpoint's wait",
            )
            backend.drainResult = BackendDrainResult.TimedOut
            backend.releaseDrain()
            assertEquals(DrainOutcome.TimedOut, withTimeout(BOUND) { drain.await() })
            // The worker takes its post-drain observation on resuming (the C
            // drain consumed the level itself), so the change is published
            // without any further activity.
            assertTrue(backend.awaitCondition("a post-drain record") { it.callOrder.count { c -> c == "snapshot" } >= records + 1 })
            assertTrue(backend.awaitCondition("the record is the observation") { _ -> endpoint.snapshots.value == ESTABLISHED })
            withTimeout(BOUND) { endpoint.awaitEstablished(BOUND) }
            endpoint.closeAndJoin()
        }

    @Test
    fun `overlapping callers are serialised so exactly one native drain is active and the second starts only after the first completes`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.holdDrain()
            val first = async(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(backend.awaitCondition("first in flight") { it.drainCalls == 1 && it.drainInFlight })
            val second = async(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(
                !backend.awaitCondition("no second native call while the first is held", ceilingMillis = 200) {
                    it.drainCalls != 1
                },
                "a second caller must suspend without starting a native drain",
            )
            assertTrue(second.isActive, "the second caller is pending, not failed")
            backend.holdDrain() // the second native call, once it starts, is held too
            backend.drainResult = BackendDrainResult.Flushed
            backend.releaseDrain()
            assertEquals(DrainOutcome.Flushed, withTimeout(BOUND) { first.await() })
            // Only now does the second native drain begin.
            assertTrue(backend.awaitCondition("second native drain starts after the first completed") { it.drainCalls == 2 })
            assertTrue(!backend.observed().drainEnteredWhileWaitParked)
            backend.drainResult = BackendDrainResult.TimedOut
            backend.releaseDrain()
            assertEquals(DrainOutcome.TimedOut, withTimeout(BOUND) { second.await() })
            assertTrue(backend.awaitCondition("the worker re-enters the wait") { it.waitParked })
            endpoint.closeAndJoin()
        }

    @Test
    fun `cancelling a queued overlapping drain before it starts makes no native call and leaves the active drain's latch and outcome untouched`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.holdDrain()
            val first = async(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(backend.awaitCondition("first in flight") { it.drainCalls == 1 })
            val second = launch(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(
                !backend.awaitCondition("second stays queued", ceilingMillis = 200) { it.drainCalls != 1 },
            )
            second.cancel()
            withTimeout(BOUND) { second.join() }
            assertTrue(second.isCancelled)
            assertTrue(backend.observed().latchSets.isEmpty(), "cancelling a queued drain never touches the latch")
            assertTrue(backend.observed().drainInFlight, "the active native drain is unaffected")
            backend.drainResult = BackendDrainResult.Flushed
            backend.releaseDrain()
            assertEquals(DrainOutcome.Flushed, withTimeout(BOUND) { first.await() })
            assertEquals(1, backend.observed().drainCalls, "the cancelled caller never started a native drain")
            endpoint.closeAndJoin()
        }

    // -- timeout validation, before any native call -----------------------------

    @Test
    fun `a negative or infinite timeout is rejected before any backend call, and ZERO is the zero-timeout probe`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertFailsWith<IllegalArgumentException> { endpoint.drain((-1).milliseconds) }
            assertFailsWith<IllegalArgumentException> { endpoint.drain(Duration.INFINITE) }
            assertFailsWith<IllegalArgumentException> { endpoint.drain(-Duration.INFINITE) }
            assertEquals(0, backend.observed().drainCalls, "an invalid timeout never reaches the backend")
            backend.drainResult = BackendDrainResult.TimedOut
            assertEquals(DrainOutcome.TimedOut, endpoint.drain(Duration.ZERO))
            assertEquals(listOf(0L), backend.observed().drainTimeouts)
            endpoint.closeAndJoin()
        }

    // -- one row per outcome ------------------------------------------------------

    @Test
    fun `a completed flush is Flushed and a bounded wait is TimedOut`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.drainResult = BackendDrainResult.Flushed
            assertEquals(DrainOutcome.Flushed, endpoint.drain(TIMEOUT))
            backend.drainResult = BackendDrainResult.TimedOut
            assertEquals(DrainOutcome.TimedOut, endpoint.drain(Duration.ZERO))
            assertEquals(listOf(TIMEOUT.inWholeMicroseconds, 0L), backend.observed().drainTimeouts)
            endpoint.closeAndJoin()
        }

    @Test
    fun `interrupt ends an in-flight drain promptly as Interrupted, and after resume a drain flushes without unrelated work`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.holdDrain()
            backend.drainResult = BackendDrainResult.Flushed
            val drain = async(Dispatchers.Default) { endpoint.drain(BOUND) }
            assertTrue(backend.awaitCondition("in flight") { it.drainInFlight })
            endpoint.interrupt() // the same latch as 1397: no parallel model
            assertEquals(DrainOutcome.Interrupted, withTimeout(BOUND) { drain.await() })
            assertEquals(listOf(true), backend.observed().latchSets)
            // While latched, a new drain answers at once without touching the transport.
            assertEquals(DrainOutcome.Interrupted, endpoint.drain(BOUND))
            endpoint.resume()
            assertEquals(DrainOutcome.Flushed, withTimeout(BOUND) { endpoint.drain(TIMEOUT) })
            endpoint.closeAndJoin()
        }

    @Test
    fun `a terminal endpoint during the drain is Closed, not an exception`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.holdDrain()
            val drain = async(Dispatchers.Default) { endpoint.drain(BOUND) }
            assertTrue(backend.awaitCondition("in flight") { it.drainInFlight })
            backend.setClosed(true) // the endpoint terminalised on the wire
            assertEquals(DrainOutcome.Closed, withTimeout(BOUND) { drain.await() })
            endpoint.closeAndJoin()
        }

    @Test
    fun `a backend that cannot prove a flush is Unsupported, never a silent success`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.drainResult = BackendDrainResult.Unsupported
            assertEquals(DrainOutcome.Unsupported, endpoint.drain(TIMEOUT))
            endpoint.closeAndJoin()
        }

    // -- discipline ------------------------------------------------------------

    @Test
    fun `after close, drain refuses with EndpointClosedException and makes no native call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.close()
            assertFailsWith<EndpointClosedException> { endpoint.drain(TIMEOUT) }
            endpoint.closeAndJoin()
            assertFailsWith<EndpointClosedException> { endpoint.drain(TIMEOUT) }
            assertEquals(0, backend.observed().drainCalls)
        }

    @Test
    fun `after a bridge quarantine, drain reports the stable failure and makes no native call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", -1002))
            val first = assertFailsWith<BridgeContractException> { endpoint.snapshot() }
            val second = assertFailsWith<BridgeContractException> { endpoint.drain(TIMEOUT) }
            assertEquals(first.failure, second.failure)
            assertEquals(0, backend.observed().drainCalls)
            val late = runCatching { endpoint.closeAndJoin() }.exceptionOrNull()
            assertTrue(late is BridgeContractException && late.failure == first.failure)
        }

    @Test
    fun `a bridge refusal of the drain row quarantines and is never a MoqResultCode`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.drainResult = BackendDrainResult.Bridge(BridgeFailure("drain", -1002))
            val thrown = assertFailsWith<BridgeContractException> { endpoint.drain(TIMEOUT) }
            assertEquals(BridgeFailure("drain", -1002), thrown.failure)
            assertTrue(
                generateSequence(thrown::class.java as Class<*>) { it.superclass }
                    .flatMap { it.declaredFields.asSequence() }
                    .none { it.type == MoqResultCode::class.java },
            )
            backend.expireWait() // the worker learns of the quarantine at its slice end
            val late = runCatching { endpoint.closeAndJoin() }.exceptionOrNull()
            assertTrue(late is BridgeContractException && late.failure == thrown.failure)
            assertEquals(0, backend.observed().stopCount)
            assertEquals(0, backend.observed().destroyCount)
        }

    @Test
    fun `a wrong-state answer is outside the contract from a Kotlin caller and quarantines, never a DrainOutcome`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.drainResult = BackendDrainResult.Bridge(BridgeFailure("drain", -5))
            val thrown = assertFailsWith<BridgeContractException> { endpoint.drain(TIMEOUT) }
            assertEquals(BridgeFailure("drain", -5), thrown.failure)
            backend.expireWait()
            val late = runCatching { endpoint.closeAndJoin() }.exceptionOrNull()
            assertTrue(late is BridgeContractException && late.failure == thrown.failure)
            assertEquals(0, backend.observed().stopCount)
        }

    @Test
    fun `the JNI backend's drain mapping is total, each documented answer to its value and everything else to Bridge`() {
        assertEquals(BackendDrainResult.Flushed, JniEndpointBackend.drainResultOf(0))
        assertEquals(BackendDrainResult.TimedOut, JniEndpointBackend.drainResultOf(1))
        assertEquals(BackendDrainResult.Interrupted, JniEndpointBackend.drainResultOf(-13))
        assertEquals(BackendDrainResult.Closed, JniEndpointBackend.drainResultOf(-4))
        assertEquals(BackendDrainResult.Unsupported, JniEndpointBackend.drainResultOf(-14))
        // MOQ_ERR_WRONG_STATE cannot come from a Kotlin thread; a registry
        // refusal is a token refusal: both are bridge failures with the code verbatim.
        assertEquals(BackendDrainResult.Bridge(BridgeFailure("drain", -5)), JniEndpointBackend.drainResultOf(-5))
        assertEquals(BackendDrainResult.Bridge(BridgeFailure("drain", -1002)), JniEndpointBackend.drainResultOf(-1002))
        assertEquals(BackendDrainResult.Bridge(BridgeFailure("drain", -777)), JniEndpointBackend.drainResultOf(-777))
    }

    @Test
    fun `a wrong-concrete-type token refusal is a bridge failure carrying the exact code, never an endpoint result`() {
        // MOQ5_ERR_REGISTRY_BASE + MOQ5_REG_ERR_TYPE: the bridge refused a
        // token of the right lifetime kind and the wrong family. It is a
        // private-registry code, so no endpoint ever answered it.
        val type = -1010
        assertTrue(JniEndpointBackend.isBridgeRefusal(type), "a concrete-type refusal belongs to the bridge's space")
        assertEquals(
            BackendDrainResult.Bridge(BridgeFailure("drain", type)),
            JniEndpointBackend.drainResultOf(type),
            "the op and the exact code must both survive; a bridge failure is never narrowed to a service refusal",
        )
        assertEquals(null, EndpointCreateFailedException(type).endpointCode, "it has no endpoint-domain meaning")
    }

    @Test
    fun `cancelling the drain coroutine detaches the caller only, and the native drain completes under engine accounting before the worker resumes`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.holdDrain()
            val job = launch(Dispatchers.Default) { endpoint.drain(BOUND) }
            assertTrue(backend.awaitCondition("in flight") { it.drainInFlight })
            job.cancel()
            withTimeout(BOUND) { job.join() }
            assertTrue(job.isCancelled)
            assertTrue(backend.observed().latchSets.isEmpty(), "cancellation is the coroutine's; the latch is the app's")
            assertTrue(backend.observed().drainInFlight, "the blocking native call is not abandoned mid-flight")
            // The worker stays quiesced until the active native drain releases.
            assertTrue(!backend.observed().waitParked)
            backend.drainResult = BackendDrainResult.Flushed
            backend.releaseDrain()
            assertTrue(backend.awaitCondition("the drain returned and the worker resumed") { !it.drainInFlight && it.waitParked })
            // Teardown discipline unchanged: stop then destroy, exactly once.
            endpoint.closeAndJoin()
            assertEquals(listOf("stop", "destroy"), backend.observed().callOrder.filter { it == "stop" || it == "destroy" })
        }

    @Test
    fun `a drain cancelled before any native call owes no post-drain observation`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            // Hold the worker INSIDE its wait: the return the drain's wake
            // produces is computed but not delivered, so the worker never
            // leaves the wait while the drain caller is alive.
            backend.holdNextWaitReturn()
            val job = launch(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(backend.awaitCondition("the wait return is held") { it.waitReturnHeld })
            job.cancel()
            withTimeout(BOUND) { job.join() }
            assertTrue(job.isCancelled)
            assertEquals(0, backend.observed().drainCalls, "cancelled before any native drain began")
            val records = backend.observed().callOrder.count { it == "snapshot" }
            backend.releaseHeldWaitReturn()
            // The held ACTIVITY return earns its one record; nothing else is
            // owed, because no native drain consumed the endpoint's wait.
            assertTrue(backend.awaitCondition("the worker is back in its wait") { it.waitParked && !it.waitReturnHeld })
            assertEquals(records + 1, backend.observed().callOrder.count { it == "snapshot" },
                "no post-drain observation for a drain that never ran")
            assertTrue(
                !backend.awaitCondition("no late record", ceilingMillis = 200) {
                    it.callOrder.count { c -> c == "snapshot" } != records + 1
                },
            )
            endpoint.closeAndJoin()
        }

    @Test
    fun `a close during the pre-entry drain window stops the endpoint but defers destroy until the drain has returned`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            // The engine has granted the native turn; the thread has not
            // reached the backend row (nothing recorded yet).
            backend.holdNextDrainEntry()
            val drain = async(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(backend.awaitCondition("held before entry") { it.drainEntryHeld })
            assertEquals(0, backend.observed().drainCalls)

            val teardown = launch(Dispatchers.Default) { endpoint.closeAndJoin() }
            // Stop may run -- it is what makes the C drain answer Closed -- but
            // destroy must wait for the granted drain turn to release.
            assertTrue(backend.awaitCondition("stop ran") { it.stopCount == 1 })
            assertTrue(
                !backend.awaitCondition("no destroy while the drain turn is held", ceilingMillis = 200) { it.destroyCount != 0 },
                "destroy crossed the pre-lease drain window",
            )

            backend.releaseDrainEntry()
            assertEquals(DrainOutcome.Closed, withTimeout(BOUND) { drain.await() },
                "a drain that reaches the row after the stop is Closed, never a bridge or closed exception")
            withTimeout(BOUND) { teardown.join() }
            val seen = backend.observed()
            assertEquals(1, seen.stopCount)
            assertEquals(1, seen.destroyCount)
            assertTrue(seen.callOrder.indexOf("drain") < seen.callOrder.indexOf("destroy"),
                "destroy must be ordered after the drain body returned; order was ${seen.callOrder}")
            assertTrue(seen.violations.isEmpty(), "no post-destroy backend call: ${seen.violations}")
        }

    @Test
    fun `a bridge refusal from a drain that retirement is waiting on quarantines, so nothing is destroyed and every waiter gets the same failure`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            // Pre-entry: the native turn is granted, the row not yet reached.
            // (An in-body hold would be ended by the stop itself, exactly as
            // the C loop answers Closed; the refusal must come from the row.)
            backend.holdNextDrainEntry()
            val drain = async(Dispatchers.Default) { runCatching { endpoint.drain(TIMEOUT) } }
            assertTrue(backend.awaitCondition("held before entry") { it.drainEntryHeld })
            val teardown = async(Dispatchers.Default) { runCatching { endpoint.closeAndJoin() } }
            // Retirement stopped the endpoint and is now waiting for the
            // granted drain turn before it may destroy.
            assertTrue(backend.awaitCondition("stop ran") { it.stopCount == 1 })
            assertTrue(!backend.awaitCondition("no destroy while the turn is held", ceilingMillis = 200) { it.destroyCount != 0 })
            // The drain returns a BRIDGE refusal of the row -- not masked into
            // Closed by the stop -- so native ownership is quarantined.
            val refusal = BridgeFailure("drain", -1002)
            backend.drainBridgeOnReturn = refusal
            backend.releaseDrainEntry()
            val drainOutcome = withTimeout(BOUND) { drain.await() }.exceptionOrNull()
            assertTrue(drainOutcome is BridgeContractException && drainOutcome.failure == refusal,
                "the drain caller gets the exact bridge failure; saw $drainOutcome")
            val closeOutcome = withTimeout(BOUND) { teardown.await() }.exceptionOrNull()
            assertTrue(closeOutcome is BridgeContractException && closeOutcome.failure == refusal,
                "closeAndJoin gets the same stable failure; saw $closeOutcome")
            assertEquals(1, backend.observed().stopCount, "the stop that already succeeded stands")
            assertEquals(0, backend.observed().destroyCount, "no destroy after a quarantine")
            // Later attempts: the same stable failure, no native call.
            val calls = backend.observed().callOrder.size
            val lateClose = runCatching { endpoint.closeAndJoin() }.exceptionOrNull()
            assertTrue(lateClose is BridgeContractException && lateClose.failure == refusal)
            val lateDrain = runCatching { endpoint.drain(TIMEOUT) }.exceptionOrNull()
            assertTrue(lateDrain is BridgeContractException && lateDrain.failure == refusal)
            assertEquals(calls, backend.observed().callOrder.size, "no further backend call after the quarantine")
            assertEquals(1, backend.observed().drainCalls)
        }

    @Test
    fun `a second closeAndJoin during a deferred destroy waits for teardown to settle and sees exactly one destroy`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            backend.holdNextDrainEntry()
            val drain = async(Dispatchers.Default) { endpoint.drain(TIMEOUT) }
            assertTrue(backend.awaitCondition("held before entry") { it.drainEntryHeld })
            val first = async(Dispatchers.Default) { runCatching { endpoint.closeAndJoin() } }
            var second: kotlinx.coroutines.Deferred<Result<Unit>>? = null
            try {
                assertTrue(backend.awaitCondition("stop ran") { it.stopCount == 1 })
                assertEquals(0, backend.observed().destroyCount)
                // Retirement is latched but NOT settled: destroy is waiting for
                // the granted drain turn. A second close must join that wait.
                second = async(Dispatchers.Default) { runCatching { endpoint.closeAndJoin() } }
                assertTrue(withTimeoutOrNull(200.milliseconds) { second.await() } == null,
                    "a second closeAndJoin completed before teardown settled (destroy has not run)")
            } finally {
                // Whatever was asserted, the held drain is released and every
                // child joined, so a failing row fails by name, never by hanging.
                backend.releaseDrainEntry()
                withTimeout(BOUND) { drain.join(); first.join(); second?.join() }
            }
            assertEquals(DrainOutcome.Closed, drain.await())
            assertTrue(first.await().isSuccess)
            assertTrue(second!!.await().isSuccess)
            val seen = backend.observed()
            assertEquals(1, seen.stopCount)
            assertEquals(1, seen.destroyCount, "both closes settle on exactly one destroy")
            assertTrue(seen.callOrder.indexOf("drain") < seen.callOrder.indexOf("destroy"))
            assertTrue(seen.violations.isEmpty(), "no post-destroy backend call: ${seen.violations}")
        }

    @Test
    fun `a second closeAndJoin during a deferred destroy gets the same quarantine failure as the first`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.awaitCondition("the wait is parked") { it.waitParked })
            backend.holdNextDrainEntry()
            val drain = async(Dispatchers.Default) { runCatching { endpoint.drain(TIMEOUT) } }
            assertTrue(backend.awaitCondition("held before entry") { it.drainEntryHeld })
            val first = async(Dispatchers.Default) { runCatching { endpoint.closeAndJoin() } }
            val refusal = BridgeFailure("drain", -1002)
            var second: kotlinx.coroutines.Deferred<Result<Unit>>? = null
            try {
                assertTrue(backend.awaitCondition("stop ran") { it.stopCount == 1 })
                second = async(Dispatchers.Default) { runCatching { endpoint.closeAndJoin() } }
                assertTrue(withTimeoutOrNull(200.milliseconds) { second.await() } == null,
                    "a second closeAndJoin completed before teardown settled (the quarantine is not known yet)")
            } finally {
                backend.drainBridgeOnReturn = refusal
                backend.releaseDrainEntry()
                withTimeout(BOUND) { drain.join(); first.join(); second?.join() }
            }
            val drainOutcome = drain.await().exceptionOrNull()
            assertTrue(drainOutcome is BridgeContractException && drainOutcome.failure == refusal)
            for ((name, close) in listOf("first" to first, "second" to second!!)) {
                val outcome = close.await().exceptionOrNull()
                assertTrue(outcome is BridgeContractException && outcome.failure == refusal,
                    "the $name closeAndJoin must carry the exact quarantine failure; saw $outcome")
            }
            assertEquals(0, backend.observed().destroyCount)
            val calls = backend.observed().callOrder.size
            assertTrue(runCatching { endpoint.closeAndJoin() }.exceptionOrNull() is BridgeContractException)
            assertEquals(calls, backend.observed().callOrder.size, "no later backend call")
        }

    @Test
    fun `closeAndJoin still never implies a drain`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.closeAndJoin()
            assertEquals(0, backend.observed().drainCalls)
        }
}
