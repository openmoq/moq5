package org.openmoq.moq5

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.EngineLedger
import org.openmoq.moq5.engine.Gate
import org.openmoq.moq5.engine.HookedBackend
import org.openmoq.moq5.engine.ManualDispatcher
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Resource-producing operations under caller cancellation, RED-first.
 *
 * Three public operations create a native resource whose only handle is the
 * value they return: [MoqEndpoint.attachReceiver] (an owned receiver child),
 * [MoqEndpoint.attachSender] (an owned sender child) and [MediaSender.addTrack]
 * (a catalog track the C registered). Source-derived: all three run through
 * `EndpointEngine.perform`, whose claimed job's result a cancelled caller
 * DROPS -- so a caller cancelled after its job was accepted can leave a
 * native resource that nobody owns, that occupies the endpoint's one slot,
 * and that only endpoint teardown ever retires.
 *
 * The intended rule is the engine's existing `performCommitted` contract:
 * cancellation BEFORE accepted submission creates nothing; accepted work
 * finishes and delivers its handle (or its error) to the caller whether the
 * cancellation landed while the job was queued, claimed, after the native
 * call succeeded, or while the result was already dispatched to the caller.
 * Ownership is what is protected, not the operation's cancellability.
 *
 * Every row records the actual returned handle in the caller's own side
 * channel: a cancelled `Deferred.await()` alone cannot tell "the value was
 * delivered" from "the value was dropped". The delivered handle is then
 * proven to be the created resource by retiring it explicitly (the exact
 * native close/remove of the created token, once), the slot is reused, and
 * the parent's teardown is exact.
 */
internal class PublicResourceCancellationTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val RX = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false)
        val TX = SenderConfig(namespace = NAMESPACE, backpressure = SendBackpressure.ReturnWouldBlock)
        val VIDEO =
            SendTrackConfig(
                name = TrackName("v".toByteArray()), mediaType = MediaType.Video, packaging = PackagingKind.Raw,
                codec = MediaBytes("avc1.64001f".toByteArray()), bitrate = 2_000_000uL,
            )
    }

    private enum class Boundary { PRE_SUBMIT, QUEUED, CLAIMED, ACCEPTED, DELIVERY }

    private class Rig(val inner: ScriptedEndpointBackend, val hooked: HookedBackend, val endpoint: MoqEndpoint)

    /** One resource-producing operation: how a row holds, drives, and judges it. */
    private abstract class Op(val name: String, val rig: Rig) {
        /** Hook the PRECEDING job's native call (a stats read on a sibling child), so the operation's job stays queued behind it. */
        abstract fun onHold(hook: () -> Unit)

        /** Hook immediately before / after the operation's own native call. */
        abstract fun onBefore(hook: () -> Unit)

        abstract fun onAfter(hook: () -> Unit)

        abstract suspend fun holder()

        abstract suspend fun submit(): Any

        abstract fun created(s: ScriptedEndpointBackend.Snapshot): Int

        abstract fun createdToken(s: ScriptedEndpointBackend.Snapshot): Long

        /** The native token a handle carries, when the handle exposes one. */
        open fun handleToken(handle: Any): Long? = null

        abstract suspend fun retire(handle: Any)

        abstract fun retired(s: ScriptedEndpointBackend.Snapshot): List<Long>

        abstract suspend fun reuse()
    }

    private class ReceiverAttachOp(rig: Rig, private val sibling: MediaSender) : Op("receiver-attach", rig) {
        override fun onHold(hook: () -> Unit) { rig.hooked.beforeSenderStats = hook }
        override fun onBefore(hook: () -> Unit) { rig.hooked.beforeAttachReceiver = hook }
        override fun onAfter(hook: () -> Unit) { rig.hooked.afterAttachReceiver = hook }
        override suspend fun holder() { sibling.statsSnapshot() }
        override suspend fun submit(): Any = rig.endpoint.attachReceiver(RX)
        override fun created(s: ScriptedEndpointBackend.Snapshot): Int = s.receiverAttaches.size
        override fun createdToken(s: ScriptedEndpointBackend.Snapshot): Long = s.receiverAttaches.last().token
        override suspend fun retire(handle: Any) { (handle as MediaReceiver).close() }
        override fun retired(s: ScriptedEndpointBackend.Snapshot): List<Long> = s.receiverCloses
        override suspend fun reuse() { rig.endpoint.attachReceiver(RX).close() }
    }

    private class SenderAttachOp(rig: Rig, private val sibling: MediaReceiver) : Op("sender-attach", rig) {
        override fun onHold(hook: () -> Unit) { rig.hooked.beforeReceiverStats = hook }
        override fun onBefore(hook: () -> Unit) { rig.hooked.beforeAttachSender = hook }
        override fun onAfter(hook: () -> Unit) { rig.hooked.afterAttachSender = hook }
        override suspend fun holder() { sibling.statsSnapshot() }
        override suspend fun submit(): Any = rig.endpoint.attachSender(TX)
        override fun created(s: ScriptedEndpointBackend.Snapshot): Int = s.senderAttaches.size
        override fun createdToken(s: ScriptedEndpointBackend.Snapshot): Long = s.senderAttaches.last().token
        override suspend fun retire(handle: Any) { (handle as MediaSender).close() }
        override fun retired(s: ScriptedEndpointBackend.Snapshot): List<Long> = s.senderCloses
        override suspend fun reuse() { rig.endpoint.attachSender(TX).close() }
    }

    private class AddTrackOp(rig: Rig, private val sender: MediaSender) : Op("sender-add-track", rig) {
        override fun onHold(hook: () -> Unit) { rig.hooked.beforeSenderStats = hook }
        override fun onBefore(hook: () -> Unit) { rig.hooked.beforeSenderAddTrack = hook }
        override fun onAfter(hook: () -> Unit) { rig.hooked.afterSenderAddTrack = hook }
        override suspend fun holder() { sender.statsSnapshot() }
        override suspend fun submit(): Any = sender.addTrack(VIDEO)
        override fun created(s: ScriptedEndpointBackend.Snapshot): Int = s.senderAddTracks.size
        override fun createdToken(s: ScriptedEndpointBackend.Snapshot): Long = s.senderAddTracks.last().track
        override fun handleToken(handle: Any): Long = (handle as SendTrack).token
        override suspend fun retire(handle: Any) { sender.removeTrack(handle as SendTrack) }
        override fun retired(s: ScriptedEndpointBackend.Snapshot): List<Long> = s.senderRemoves.map { it.track }
        override suspend fun reuse() { sender.removeTrack(sender.addTrack(VIDEO)) }
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun rig(): Rig {
        val inner = newBackend()
        val hooked = HookedBackend(inner)
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { hooked }, inner)
        return Rig(inner, hooked, endpoint)
    }

    private suspend fun receiverAttach(rig: Rig): Op = ReceiverAttachOp(rig, rig.endpoint.attachSender(TX))

    private suspend fun senderAttach(rig: Rig): Op = SenderAttachOp(rig, rig.endpoint.attachReceiver(RX))

    private suspend fun addTrack(rig: Rig): Op = AddTrackOp(rig, rig.endpoint.attachSender(TX))

    /** The delivered handle IS the created resource: retire it through the handle, exactly once, then reuse the slot. */
    private suspend fun judgeDelivered(op: Op, slot: CompletableDeferred<Result<Any>>, what: String) {
        val r = withTimeout(BOUND) { slot.await() }
        val s = op.rig.inner.observed()
        assertTrue(
            r.isSuccess,
            "R3 ${op.name} $what: the accepted job created ${op.created(s)} native resource(s), but the cancelled caller received " +
                "${r.exceptionOrNull()} instead of the handle -- the resource is owned by nobody and occupies the slot until endpoint teardown",
        )
        val handle = r.getOrThrow()
        assertEquals(1, op.created(s), "R3 ${op.name} $what: exactly one native resource was created")
        val token = op.createdToken(s)
        op.handleToken(handle)?.let { assertEquals(token, it, "R3 ${op.name} $what: the delivered handle carries the created token") }
        withTimeout(BOUND) { op.retire(handle) }
        assertEquals(listOf(token), op.retired(op.rig.inner.observed()), "R3 ${op.name} $what: explicit retirement through the handle retires exactly the created resource, once")
        withTimeout(BOUND) { op.reuse() }
        assertEquals(emptyList(), op.rig.inner.observed().violations, "R3 ${op.name} $what: backend violations")
    }

    /**
     * Apply the caller's cancellation INSIDE the operation's window, on the
     * worker, at the hook [install] names (before or after the native call),
     * and return how many native resources existed at that instant.
     *
     * The window is entered by the worker; the sender operations run their
     * native call while holding the engine lock, so a cancel issued from the
     * application thread would only have its engine-side handler applied
     * after the window closed -- the test thread would block on that lock
     * instead of observing the boundary. `Job.cancel()` is thread-agnostic:
     * applied here, the caller's continuation is cancelled deterministically
     * at exactly the claimed boundary, and the handler the engine registered
     * runs reentrantly. The receiver and sender attaches, whose native call
     * runs outside the lock, are cancelled the same way for one shape.
     */
    private suspend fun CoroutineScope.cancelInsideWindow(
        op: Op,
        ledger: EngineLedger,
        gates: MutableList<Gate>,
        slot: CompletableDeferred<Result<Any>>,
        install: (() -> Unit) -> Unit,
        label: String,
    ): Int {
        val callerReady = gate("${op.name}-$label-caller").also { gates += it }
        val callerRef = AtomicReference<Job?>(null)
        val createdAtCancel = AtomicInteger(-1)
        install {
            ledger.started(label)
            if (callerReady.await()) {
                createdAtCancel.set(op.created(op.rig.inner.observed()))
                callerRef.get()!!.cancel()
            }
            ledger.completed(label)
        }
        val caller = launch(Dispatchers.Default) { slot.complete(runCatching { op.submit() }) }
        callerRef.set(caller)
        callerReady.open()
        assertTrue(ledger.await("the cancel is applied inside the $label window") { it.completed.contains(label) }, "R3 ${op.name} $label: the job never reached the window")
        return createdAtCancel.get()
    }

    private fun row(boundary: Boundary, build: suspend (Rig) -> Op) =
        runTest {
            val rig = rig()
            val gates = mutableListOf<Gate>()
            run {
                val op = build(rig)
                val slot = CompletableDeferred<Result<Any>>()
                val ledger = ledger()
                when (boundary) {
                    Boundary.PRE_SUBMIT -> {
                        val wakes = rig.inner.observed().wakeCount
                        val caller =
                            launch(Dispatchers.Default) {
                                coroutineContext[Job]!!.cancel()
                                slot.complete(runCatching { op.submit() })
                            }
                        withTimeout(BOUND) { caller.join() }
                        val r = withTimeout(BOUND) { slot.await() }
                        assertTrue(r.exceptionOrNull() is CancellationException, "R3 ${op.name} pre-submit: a caller cancelled before submission resolves as cancellation; got $r")
                        assertEquals(0, op.created(rig.inner.observed()), "R3 ${op.name} pre-submit: nothing is created")
                        assertEquals(wakes, rig.inner.observed().wakeCount, "R3 ${op.name} pre-submit: nothing is submitted, so nothing wakes")
                    }
                    Boundary.QUEUED -> {
                        val hold = gate("${op.name}-hold").also { gates += it }
                        op.onHold { ledger.started("held"); hold.await() }
                        val holder = async(Dispatchers.Default) { op.holder() }
                        assertTrue(ledger.await("the preceding job is claimed") { it.started.contains("held") }, "R3 ${op.name} queued: the holding job never ran")
                        val wakes = rig.inner.observed().wakeCount
                        val caller = launch(Dispatchers.Default) { slot.complete(runCatching { op.submit() }) }
                        assertTrue(rig.inner.awaitCondition("the submission is accepted") { it.wakeCount == wakes + 1 }, "R3 ${op.name} queued: the submission was never accepted")
                        assertEquals(0, op.created(rig.inner.observed()), "R3 ${op.name} queued: still queued behind the held job")
                        caller.cancel()
                        hold.open()
                        withTimeout(BOUND) { holder.await() }
                        judgeDelivered(op, slot, "queued")
                    }
                    Boundary.CLAIMED -> {
                        val createdAtCancel = cancelInsideWindow(op, ledger, gates, slot, install = op::onBefore, label = "claimed")
                        assertEquals(0, createdAtCancel, "R3 ${op.name} claimed: the cancel was applied before the native mutation")
                        judgeDelivered(op, slot, "claimed-before-mutation")
                    }
                    Boundary.ACCEPTED -> {
                        val createdAtCancel = cancelInsideWindow(op, ledger, gates, slot, install = op::onAfter, label = "accepted")
                        assertEquals(1, createdAtCancel, "R3 ${op.name} accepted: the native resource existed when the cancel was applied, before the body returned")
                        judgeDelivered(op, slot, "backend-success-before-return")
                    }
                    Boundary.DELIVERY -> {
                        // A held predecessor keeps the operation's job from
                        // running while the manual caller is started, so the
                        // caller is provably SUSPENDED before any result can
                        // exist: the worker cannot finish the body -- and the
                        // value cannot return synchronously -- during runOne().
                        val hold = gate("${op.name}-delivery-hold").also { gates += it }
                        op.onHold { ledger.started("held"); hold.await() }
                        val holder = async(Dispatchers.Default) { op.holder() }
                        assertTrue(ledger.await("the preceding job is claimed") { it.started.contains("held") }, "R3 ${op.name} delivery: the holding job never ran")
                        val manual = ManualDispatcher()
                        val scope = manualScope(manual)
                        val wakes = rig.inner.observed().wakeCount
                        val caller = scope.launch { slot.complete(runCatching { op.submit() }) }
                        assertTrue(manual.runOne(), "R3 ${op.name} delivery: the caller's start was dispatched")
                        assertTrue(rig.inner.awaitCondition("the submission is accepted") { it.wakeCount == wakes + 1 }, "R3 ${op.name} delivery: the submission was never accepted")
                        assertEquals(0, op.created(rig.inner.observed()), "R3 ${op.name} delivery: no resource yet behind the held predecessor")
                        assertTrue(caller.isActive && !slot.isCompleted && manual.queued() == 0, "R3 ${op.name} delivery: the caller is suspended with nothing dispatched")
                        hold.open()
                        withTimeout(BOUND) { holder.await() }
                        assertTrue(rig.inner.awaitCondition("the body ran on the worker") { op.created(it) == 1 }, "R3 ${op.name} delivery: the native resource was never created")
                        assertTrue(manual.awaitDispatched(2), "R3 ${op.name} delivery: the worker never dispatched the result to the caller's dispatcher")
                        assertEquals(1, manual.queued(), "R3 ${op.name} delivery: exactly the single first-result resumption is queued, not yet run")
                        assertTrue(!slot.isCompleted, "R3 ${op.name} delivery: the result has not reached the caller")
                        // The value is dispatched but the caller has not resumed: cancel in that window.
                        caller.cancel()
                        manual.runAll()
                        judgeDelivered(op, slot, "dispatcher-delivery")
                    }
                }
            }
            gates.forEach { it.open() }
            // Parent teardown (one stop, one destroy, exact worker join, no
            // violations) is the fixture's unconditional audit of the owned
            // endpoint: it runs whether or not this row's RED fired.
        }

    // -- receiver attach --------------------------------------------------------

    @Test
    fun `receiver attach -- cancelled before submission creates nothing and wakes nothing`() = row(Boundary.PRE_SUBMIT, ::receiverAttach)

    @Test
    fun `receiver attach -- cancelled while queued behind a held job still delivers the attached receiver`() = row(Boundary.QUEUED, ::receiverAttach)

    @Test
    fun `receiver attach -- cancelled after the claim and before the native call still delivers the attached receiver`() = row(Boundary.CLAIMED, ::receiverAttach)

    @Test
    fun `receiver attach -- cancelled after the native attach succeeded and before the body returns still delivers the receiver`() = row(Boundary.ACCEPTED, ::receiverAttach)

    @Test
    fun `receiver attach -- cancelled while the result is dispatched to the caller's dispatcher still delivers the receiver`() = row(Boundary.DELIVERY, ::receiverAttach)

    // -- sender attach ----------------------------------------------------------

    @Test
    fun `sender attach -- cancelled before submission creates nothing and wakes nothing`() = row(Boundary.PRE_SUBMIT, ::senderAttach)

    @Test
    fun `sender attach -- cancelled while queued behind a held job still delivers the attached sender`() = row(Boundary.QUEUED, ::senderAttach)

    @Test
    fun `sender attach -- cancelled after the claim and before the native call still delivers the attached sender`() = row(Boundary.CLAIMED, ::senderAttach)

    @Test
    fun `sender attach -- cancelled after the native attach succeeded and before the body returns still delivers the sender`() = row(Boundary.ACCEPTED, ::senderAttach)

    @Test
    fun `sender attach -- cancelled while the result is dispatched to the caller's dispatcher still delivers the sender`() = row(Boundary.DELIVERY, ::senderAttach)

    // -- sender addTrack --------------------------------------------------------

    @Test
    fun `addTrack -- cancelled before submission creates nothing and wakes nothing`() = row(Boundary.PRE_SUBMIT, ::addTrack)

    @Test
    fun `addTrack -- cancelled while queued behind a held job still delivers the sender-owned track handle`() = row(Boundary.QUEUED, ::addTrack)

    @Test
    fun `addTrack -- cancelled after the claim and before the native call still delivers the sender-owned track handle`() = row(Boundary.CLAIMED, ::addTrack)

    @Test
    fun `addTrack -- cancelled after the native add succeeded and before the body returns still delivers the track handle`() = row(Boundary.ACCEPTED, ::addTrack)

    @Test
    fun `addTrack -- cancelled while the result is dispatched to the caller's dispatcher still delivers the track handle`() = row(Boundary.DELIVERY, ::addTrack)
}
