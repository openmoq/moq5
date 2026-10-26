package org.openmoq.moq5

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendWaitResult
import org.openmoq.moq5.engine.BackendWriteResult
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.HookedBackend
import org.openmoq.moq5.engine.ManualDispatcher
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Ownership of RETAINED work under contest, RED-first.
 *
 * Two boundaries of the worker-retained retry that the quiescence and
 * progress rows do not reach:
 *
 *  * A retained sender attempt (the retry after an initial WouldBlock) can
 *    be in its native call when the caller's cancellation or timeout arrives.
 *    Once that call commits Accepted, native ownership transferred, so the
 *    public result must be Accepted -- exactly one transfer, no later attempt
 *    -- whichever side wins the race to retire the entry. Between attempts
 *    the cancellation must still win and transfer nothing. The retirement
 *    request is registered while the attempt is in flight (the caller is
 *    stepped on a manual dispatcher from an owned helper thread, so a
 *    retirement that BLOCKS instead of registering is a named failure, not a
 *    hang), and the caller's final answer waits for that attempt to settle.
 *  * Public `MoqEndpoint.close()` publishes its own teardown wake. That wake
 *    must not be turned into another native attempt for retained work: a
 *    retained write or demand parked at close resolves with its documented
 *    public sender-close / stream-end outcome, never the internal engine
 *    exception, and teardown retires everything once. An attempt already in
 *    flight at close that commits still delivers Accepted exactly once.
 */
internal class PublicRetainedOwnershipTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val RX = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false)
        val TX = SenderConfig(namespace = NAMESPACE, backpressure = SendBackpressure.ReturnWouldBlock)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val VIDEO_RX = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        val VIDEO_TX =
            SendTrackConfig(
                name = TrackName("v".toByteArray()), mediaType = MediaType.Video, packaging = PackagingKind.Raw,
                codec = MediaBytes("avc1.64001f".toByteArray()), bitrate = 2_000_000uL,
            )
        val CAPTURE = UnixMicros(1_700_000_000_000_009uL)
        val DATA =
            BackendObject(
                track = T1, payload = MediaPayload.Raw(MediaBytes("data".toByteArray())), status = ObjectStatus.Normal, endOfGroup = false,
                viaDatagram = false, keyframe = true, captureTime = CAPTURE,
                decodeTime = MediaMicros(CAPTURE.value), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(CAPTURE.value),
            )
        val OBJ = SendObject(payload = MediaBytes("k".toByteArray()), isSync = true, startsGroup = true, presentationTime = MediaMicros(40_000uL))
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)
    }

    private class TxRig(val inner: ScriptedEndpointBackend, val endpoint: MoqEndpoint, val sender: MediaSender, val track: SendTrack)

    private class RxRig(val inner: ScriptedEndpointBackend, val endpoint: MoqEndpoint, val receiver: MediaReceiver, val track: MediaTrack)

    /** Which retained sender operation a row drives; both share one shape. */
    private enum class SenderOp { Write, EndTrack }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun txRig(): TxRig {
        val inner = newBackend()
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { HookedBackend(inner) }, inner)
        val sender = endpoint.attachSender(TX)
        val track = withTimeout(BOUND) { sender.addTrack(VIDEO_TX) }
        return TxRig(inner, endpoint, sender, track)
    }

    private suspend fun rxRig(): RxRig {
        val inner = newBackend()
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { HookedBackend(inner) }, inner)
        val receiver = endpoint.attachReceiver(RX)
        inner.scriptTrackEvents(added(T1, VIDEO_RX), ready)
        inner.markActivity()
        val track = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }.tracks.single()
        return RxRig(inner, endpoint, receiver, track)
    }

    private fun ScriptedEndpointBackend.awaitParked(what: String): Boolean = awaitCondition(what) { it.waitParked && !it.activityLevel }

    private fun TxRig.setDefault(op: SenderOp, r: BackendWriteResult) {
        when (op) {
            SenderOp.Write -> inner.senderWriteDefault = r
            SenderOp.EndTrack -> inner.senderEndTrackDefault = r
        }
    }

    private fun TxRig.attempts(op: SenderOp): Int =
        when (op) {
            SenderOp.Write -> inner.observed().senderWrites.size
            SenderOp.EndTrack -> inner.observed().senderEndTracks.size
        }

    private fun TxRig.holdNext(op: SenderOp) {
        when (op) {
            SenderOp.Write -> inner.holdNextSenderWrite()
            SenderOp.EndTrack -> inner.holdNextSenderEndTrack()
        }
    }

    private fun TxRig.awaitHeld(op: SenderOp): Boolean =
        when (op) {
            SenderOp.Write -> inner.awaitCondition("the retained attempt is held inside its native call") { it.senderWriteHeld }
            SenderOp.EndTrack -> inner.awaitCondition("the retained attempt is held inside its native call") { it.senderEndTrackHeld }
        }

    private fun TxRig.release(op: SenderOp) {
        when (op) {
            SenderOp.Write -> inner.releaseSenderWrite()
            SenderOp.EndTrack -> inner.releaseSenderEndTrack()
        }
    }

    /** What transferred: the accepted payloads for a write, the accepted end marker (the handle's `isEnded`) for an end-track. */
    private fun TxRig.transferred(op: SenderOp): Int =
        when (op) {
            SenderOp.Write -> inner.observed().acceptedPayloads.size
            SenderOp.EndTrack -> if (track.isEnded) 1 else 0
        }

    private suspend fun TxRig.publicOp(op: SenderOp, timeout: Duration): WriteOutcome =
        when (op) {
            SenderOp.Write -> sender.write(track, OBJ, timeout)
            SenderOp.EndTrack -> sender.endTrack(track, timeout)
        }

    // -- F1: a committed retained attempt under contested retirement ----------------

    /**
     * The retained attempt is in its (held) native call when the caller is
     * cancelled. The caller is stepped on a manual dispatcher by an owned
     * helper thread: registering the retirement request must not need the
     * engine lock the worker holds around the call, so a helper still alive
     * after the bound is a named failure ("the retirement blocked"). Then the
     * call commits Accepted; the caller's answer, taken after the attempt
     * settled, must be Accepted with exactly one transfer and no later attempt.
     */
    private fun committedUnderContest(op: SenderOp) =
        runTest {
            val row = "F1 ${op.name.lowercase()}/committed-under-contest"
            val rig = txRig()
            rig.setDefault(op, BackendWriteResult.WouldBlock)
            val manual = ManualDispatcher()
            val scope = manualScope(manual)
            val slot = CompletableDeferred<Result<WriteOutcome>>()
            val caller = scope.launch { slot.complete(runCatching { rig.publicOp(op, 10.seconds) }) }
            // Keep the initial call pending until runOne returns. Otherwise a
            // fast worker can resume inline, so no second dispatch is owed.
            rig.holdNext(op)
            try {
                assertTrue(manual.runOne(), "$row: the caller starts")
                assertTrue(rig.awaitHeld(op), "$row: the initial attempt enters its native call")
            } finally {
                rig.release(op)
            }
            assertTrue(rig.inner.awaitCondition("the first attempt answered WouldBlock") { rig.attempts(op) == 1 }, "$row: the first attempt never ran")
            assertTrue(manual.awaitDispatched(2), "$row: the retained pending answer is dispatched to the caller")
            assertTrue(manual.runOne(), "$row: the caller now awaits its retained attempt")
            assertTrue(caller.isActive && !slot.isCompleted && manual.queued() == 0, "$row: the caller is suspended on the retained attempt")
            assertTrue(rig.inner.awaitParked("the worker parks with the attempt retained"), "$row: no park after retention")
            // The retry: genuine activity, and the retained attempt is HELD inside its native call.
            rig.setDefault(op, BackendWriteResult.Accepted)
            rig.holdNext(op)
            rig.inner.markActivity()
            var released = false
            try {
                assertTrue(rig.awaitHeld(op), "$row: the retained attempt never entered its native call")
                assertEquals(1, rig.attempts(op), "$row: the held attempt has not answered yet")
                // Contest: cancel the caller while the attempt is in flight, and
                // step its cancellation on an owned helper so a blocking
                // retirement is reported rather than deadlocking this thread.
                caller.cancel()
                assertTrue(manual.awaitDispatched(3), "$row: the caller's cancellation resumption is dispatched")
                val stepper = callBlocking("$row-retire-step") { manual.runOne() }
                val stepped = stepper.consume(ceilingMillis = 2_000)
                assertTrue(
                    stepped is CallerOutcome.NormalReturn,
                    "$row: registering the retirement while the attempt is in flight must not block on the engine lock the worker holds around the native call; the caller's step did not return ($stepped)",
                )
                assertTrue(!slot.isCompleted, "$row: the caller's final answer waits for the in-flight attempt to settle")
            } finally {
                rig.release(op)
                released = true
            }
            assertTrue(released)
            // The attempt commits; the caller learns the transfer.
            assertTrue(rig.inner.awaitCondition("the retained attempt committed") { rig.attempts(op) == 2 }, "$row: the held attempt never answered")
            assertTrue(manual.awaitDispatched(4), "$row: the settled answer is dispatched to the caller")
            manual.runAll()
            val r = withTimeout(BOUND) { slot.await() }
            assertEquals(WriteOutcome.Accepted, r.getOrNull(), "$row: native ownership transferred in the retained attempt, so the public result is Accepted; got $r")
            assertEquals(1, rig.transferred(op), "$row: exactly one transfer")
            // No later attempt: the entry is retired; a further pass makes none.
            assertTrue(rig.inner.awaitParked("the worker parks after the settled attempt"), "$row: no park after settlement")
            val before = rig.inner.observed()
            rig.inner.markActivity()
            assertTrue(rig.inner.awaitCondition("the later activity is consumed and a new empty park follows") { it.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } == before.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } + 1 && it.waitEntries == before.waitEntries + 1 && it.waitParked && !it.activityLevel })
            assertEquals(2, rig.attempts(op), "$row: no attempt after the committed one")
            assertEquals(1, rig.transferred(op), "$row: still exactly one transfer")
        }

    @Test
    fun `write -- a retained attempt that commits while its caller is being cancelled still delivers Accepted once, and nothing follows`() = committedUnderContest(SenderOp.Write)

    @Test
    fun `endTrack -- a retained attempt that commits while its caller is being cancelled still delivers Accepted once, and nothing follows`() = committedUnderContest(SenderOp.EndTrack)

    /** Between attempts nothing is in flight: the cancellation wins, the entry is retired, and no transfer ever happens. */
    private fun cancelledBetweenAttempts(op: SenderOp) =
        runTest {
            val row = "F1 ${op.name.lowercase()}/between-attempts"
            val rig = txRig()
            rig.setDefault(op, BackendWriteResult.WouldBlock)
            val manual = ManualDispatcher()
            val scope = manualScope(manual)
            val slot = CompletableDeferred<Result<WriteOutcome>>()
            val caller = scope.launch { slot.complete(runCatching { rig.publicOp(op, 10.seconds) }) }
            // Keep the initial call pending until runOne returns. Otherwise a
            // fast worker can resume inline, so no second dispatch is owed.
            rig.holdNext(op)
            try {
                assertTrue(manual.runOne(), "$row: the caller starts")
                assertTrue(rig.awaitHeld(op), "$row: the initial attempt enters its native call")
            } finally {
                rig.release(op)
            }
            assertTrue(rig.inner.awaitCondition("the first attempt answered WouldBlock") { rig.attempts(op) == 1 })
            assertTrue(manual.awaitDispatched(2) && manual.runOne(), "$row: the caller awaits its retained attempt")
            assertTrue(rig.inner.awaitParked("parked with the attempt retained"))
            rig.setDefault(op, BackendWriteResult.Accepted) // capacity exists, but no attempt may use it after the cancellation
            caller.cancel()
            assertTrue(manual.awaitDispatched(3), "$row: the cancellation resumption is dispatched")
            manual.runAll()
            val r = withTimeout(BOUND) { slot.await() }
            assertTrue(r.exceptionOrNull() is CancellationException, "$row: between attempts the cancellation wins; got $r")
            val before = rig.inner.observed()
            rig.inner.markActivity()
            assertTrue(rig.inner.awaitCondition("the later activity is consumed and a new empty park follows") { it.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } == before.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } + 1 && it.waitEntries == before.waitEntries + 1 && it.waitParked && !it.activityLevel })
            assertEquals(1, rig.attempts(op), "$row: no attempt after the cancellation")
            assertEquals(0, rig.transferred(op), "$row: nothing transferred")
        }

    @Test
    fun `write -- cancelled between attempts, the cancellation wins and nothing transfers`() = cancelledBetweenAttempts(SenderOp.Write)

    @Test
    fun `endTrack -- cancelled between attempts, the cancellation wins and nothing transfers`() = cancelledBetweenAttempts(SenderOp.EndTrack)

    // -- F3: the settled outcome of an in-flight attempt under contest -------------

    /**
     * The retained attempt is in its native call when the caller is
     * cancelled; the call then answers a REFUSAL. The settled outcome is
     * that typed refusal, not the caller's cancellation and not WouldBlock:
     * the attempt already happened, so its answer is what the caller must
     * learn. Nothing transferred, no later attempt.
     */
    private fun refusedUnderContest(op: SenderOp, code: Int, expected: Class<out Throwable>) =
        runTest {
            val row = "F3 ${op.name.lowercase()}/refused-under-contest($code)"
            val rig = txRig()
            rig.setDefault(op, BackendWriteResult.WouldBlock)
            val manual = ManualDispatcher()
            val scope = manualScope(manual)
            val slot = CompletableDeferred<Result<WriteOutcome>>()
            val caller = scope.launch { slot.complete(runCatching { rig.publicOp(op, 10.seconds) }) }
            // Keep the initial call pending until runOne returns. Otherwise a
            // fast worker can resume inline, so no second dispatch is owed.
            rig.holdNext(op)
            try {
                assertTrue(manual.runOne(), "$row: the caller starts")
                assertTrue(rig.awaitHeld(op), "$row: the initial attempt enters its native call")
            } finally {
                rig.release(op)
            }
            assertTrue(rig.inner.awaitCondition("the first attempt answered WouldBlock") { rig.attempts(op) == 1 }, "$row: the first attempt never ran")
            assertTrue(manual.awaitDispatched(2) && manual.runOne(), "$row: the caller awaits its retained attempt")
            assertTrue(rig.inner.awaitParked("retained and parked"), "$row: no park after retention")
            rig.setDefault(op, BackendWriteResult.Refused(MoqResultCode(code)))
            rig.holdNext(op)
            rig.inner.markActivity()
            var released = false
            try {
                assertTrue(rig.awaitHeld(op), "$row: the retained attempt never entered its native call")
                caller.cancel()
                assertTrue(manual.awaitDispatched(3), "$row: the caller's cancellation resumption is dispatched")
                val stepper = callBlocking("$row-retire-step") { manual.runOne() }
                assertTrue(stepper.consume(ceilingMillis = 2_000) is CallerOutcome.NormalReturn, "$row: registering the retirement must not block on the engine lock")
                assertTrue(!slot.isCompleted, "$row: the answer waits for the in-flight attempt to settle")
            } finally {
                rig.release(op)
                released = true
            }
            assertTrue(released)
            assertTrue(rig.inner.awaitCondition("the retained attempt answered") { rig.attempts(op) == 2 }, "$row: the held attempt never answered")
            assertTrue(manual.awaitDispatched(4), "$row: the settled answer is dispatched to the caller")
            manual.runAll()
            val r = withTimeout(BOUND) { slot.await() }
            val t = r.exceptionOrNull()
            assertTrue(t != null && expected.isInstance(t), "$row: the in-flight refusal settles as its typed outcome ${expected.simpleName}, not the cancellation and not WouldBlock; got $r")
            assertEquals(0, rig.transferred(op), "$row: nothing transferred")
            assertTrue(rig.inner.awaitParked("parked after the settled refusal"), "$row: no park after settlement")
            val before = rig.inner.observed()
            rig.setDefault(op, BackendWriteResult.Accepted)
            rig.inner.markActivity()
            assertTrue(rig.inner.awaitCondition("the later activity is consumed and a new empty park follows") { it.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } == before.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } + 1 && it.waitEntries == before.waitEntries + 1 && it.waitParked && !it.activityLevel })
            assertEquals(2, rig.attempts(op), "$row: no attempt after the settled refusal")
        }

    @Test
    fun `write -- an in-flight INTERRUPTED refusal settles as SenderInterruptedException under a contested retirement`() = refusedUnderContest(SenderOp.Write, -13, SenderInterruptedException::class.java)

    @Test
    fun `endTrack -- an in-flight INTERRUPTED refusal settles as SenderInterruptedException under a contested retirement`() = refusedUnderContest(SenderOp.EndTrack, -13, SenderInterruptedException::class.java)

    @Test
    fun `write -- an in-flight hard refusal (INVAL) settles as ObjectRejectedException under a contested retirement`() = refusedUnderContest(SenderOp.Write, -2, ObjectRejectedException::class.java)

    @Test
    fun `endTrack -- an in-flight hard refusal (WRONG_STATE) settles as SendTrackUnavailableException under a contested retirement`() = refusedUnderContest(SenderOp.EndTrack, -5, SendTrackUnavailableException::class.java)

    /** The in-flight attempt answers WouldBlock: nothing happened natively, so the caller's cancellation wins and no later attempt follows. */
    private fun wouldBlockUnderContest(op: SenderOp) =
        runTest {
            val row = "F3 ${op.name.lowercase()}/wouldblock-under-contest"
            val rig = txRig()
            rig.setDefault(op, BackendWriteResult.WouldBlock)
            val manual = ManualDispatcher()
            val scope = manualScope(manual)
            val slot = CompletableDeferred<Result<WriteOutcome>>()
            val caller = scope.launch { slot.complete(runCatching { rig.publicOp(op, 10.seconds) }) }
            // Keep the initial call pending until runOne returns. Otherwise a
            // fast worker can resume inline, so no second dispatch is owed.
            rig.holdNext(op)
            try {
                assertTrue(manual.runOne(), "$row: the caller starts")
                assertTrue(rig.awaitHeld(op), "$row: the initial attempt enters its native call")
            } finally {
                rig.release(op)
            }
            assertTrue(rig.inner.awaitCondition("the first attempt answered WouldBlock") { rig.attempts(op) == 1 })
            assertTrue(manual.awaitDispatched(2) && manual.runOne(), "$row: the caller awaits its retained attempt")
            assertTrue(rig.inner.awaitParked("retained and parked"))
            rig.holdNext(op) // the next attempt answers WouldBlock again (the default), held inside the call
            rig.inner.markActivity()
            var released = false
            try {
                assertTrue(rig.awaitHeld(op), "$row: the retained attempt never entered its native call")
                caller.cancel()
                assertTrue(manual.awaitDispatched(3))
                val stepper = callBlocking("$row-retire-step") { manual.runOne() }
                assertTrue(stepper.consume(ceilingMillis = 2_000) is CallerOutcome.NormalReturn, "$row: registering the retirement must not block")
                assertTrue(!slot.isCompleted, "$row: the answer waits for the in-flight attempt to settle")
            } finally {
                rig.release(op)
                released = true
            }
            assertTrue(released)
            assertTrue(rig.inner.awaitCondition("the retained attempt answered") { rig.attempts(op) == 2 })
            assertTrue(manual.awaitDispatched(4))
            manual.runAll()
            val r = withTimeout(BOUND) { slot.await() }
            assertTrue(r.exceptionOrNull() is CancellationException, "$row: an in-flight WouldBlock transferred nothing, so the cancellation wins; got $r")
            assertEquals(0, rig.transferred(op), "$row: nothing transferred")
            rig.setDefault(op, BackendWriteResult.Accepted)
            val before = rig.inner.observed()
            rig.inner.markActivity()
            assertTrue(rig.inner.awaitCondition("the later activity is consumed and a new empty park follows") { it.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } == before.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } + 1 && it.waitEntries == before.waitEntries + 1 && it.waitParked && !it.activityLevel })
            assertEquals(2, rig.attempts(op), "$row: the retired entry makes no later attempt")
            assertEquals(0, rig.transferred(op), "$row: still nothing transferred")
        }

    @Test
    fun `write -- an in-flight WouldBlock under a contested retirement lets the cancellation win, and nothing follows`() = wouldBlockUnderContest(SenderOp.Write)

    @Test
    fun `endTrack -- an in-flight WouldBlock under a contested retirement lets the cancellation win, and nothing follows`() = wouldBlockUnderContest(SenderOp.EndTrack)

    // -- F2: logical endpoint close and retained work --------------------------------

    /**
     * A retained attempt is parked; the scripted next attempt would be
     * Accepted, but the only wake is public close's own teardown wake. That
     * wake must not become another native attempt: no transfer, the public
     * operation resolves with the sender-close outcome, teardown retires
     * everything once.
     */
    private fun closeRetiresParkedAttempt(op: SenderOp) =
        runTest {
            val row = "F2 ${op.name.lowercase()}/close-parked"
            val rig = txRig()
            rig.setDefault(op, BackendWriteResult.WouldBlock)
            val slot = CompletableDeferred<Result<WriteOutcome>>()
            val caller = launch(Dispatchers.Default) { slot.complete(runCatching { rig.publicOp(op, Duration.INFINITE) }) }
            assertTrue(rig.inner.awaitCondition("the first attempt answered WouldBlock") { rig.attempts(op) == 1 }, "$row: the first attempt never ran")
            assertTrue(rig.inner.awaitParked("the attempt is retained and the worker parked"), "$row: no park after retention")
            rig.setDefault(op, BackendWriteResult.Accepted) // the next attempt WOULD be accepted; nothing but close wakes the worker
            rig.endpoint.close()
            val r = withTimeout(BOUND) { slot.await() }
            assertTrue(r.exceptionOrNull() is SenderClosedException, "$row: the documented public outcome of a retained operation at close is SenderClosedException; got $r")
            withTimeout(BOUND) { caller.join() }
            withTimeout(BOUND) { rig.endpoint.closeAndJoin() }
            val s = rig.inner.observed()
            assertEquals(1, rig.attempts(op), "$row: the teardown wake was turned into another native attempt")
            assertEquals(0, rig.transferred(op), "$row: nothing transferred after close began")
            assertEquals(1, s.senderCloses.size, "$row: the sender retired once")
            assertEquals(1, s.stopCount, "$row: one stop")
            assertEquals(1, s.destroyCount, "$row: one destroy")
        }

    @Test
    fun `write -- endpoint close does not turn its teardown wake into another retained attempt, and the caller gets the sender-close outcome`() = closeRetiresParkedAttempt(SenderOp.Write)

    @Test
    fun `endTrack -- endpoint close does not turn its teardown wake into another retained attempt, and the caller gets the sender-close outcome`() = closeRetiresParkedAttempt(SenderOp.EndTrack)

    /** Close while the retained attempt is already in flight and commits: ownership wins, Accepted exactly once. */
    private fun closeDuringInFlightCommit(op: SenderOp) =
        runTest {
            val row = "F2 ${op.name.lowercase()}/close-in-flight"
            val rig = txRig()
            rig.setDefault(op, BackendWriteResult.WouldBlock)
            val slot = CompletableDeferred<Result<WriteOutcome>>()
            val caller = launch(Dispatchers.Default) { slot.complete(runCatching { rig.publicOp(op, Duration.INFINITE) }) }
            assertTrue(rig.inner.awaitCondition("the first attempt answered WouldBlock") { rig.attempts(op) == 1 })
            assertTrue(rig.inner.awaitParked("retained and parked"))
            rig.setDefault(op, BackendWriteResult.Accepted)
            rig.holdNext(op)
            rig.inner.markActivity()
            // The worker holds the engine lock across a native sender call, so
            // the close request is issued from an owned helper: it arrives
            // while the attempt is in flight and is applied once the call ends.
            val closer: BlockingCaller
            var released = false
            try {
                assertTrue(rig.awaitHeld(op), "$row: the retained attempt never entered its native call")
                closer = callBlocking("$row-close") { rig.endpoint.close() }
                assertTrue(!slot.isCompleted, "$row: nothing is answered while the attempt is in flight")
            } finally {
                rig.release(op)
                released = true
            }
            assertTrue(released)
            assertTrue(closer.consume() is CallerOutcome.NormalReturn, "$row: the close request was applied once the native call ended")
            val r = withTimeout(BOUND) { slot.await() }
            assertEquals(WriteOutcome.Accepted, r.getOrNull(), "$row: the attempt committed before close could stop it, so Accepted is delivered; got $r")
            withTimeout(BOUND) { caller.join() }
            withTimeout(BOUND) { rig.endpoint.closeAndJoin() }
            assertEquals(2, rig.attempts(op), "$row: the committed attempt and nothing after it")
            assertEquals(1, rig.transferred(op), "$row: exactly one transfer")
        }

    @Test
    fun `write -- close during an in-flight retained attempt that commits still delivers Accepted exactly once`() = closeDuringInFlightCommit(SenderOp.Write)

    @Test
    fun `endTrack -- close during an in-flight retained attempt that commits still delivers Accepted exactly once`() = closeDuringInFlightCommit(SenderOp.EndTrack)

    @Test
    fun `receiver -- endpoint close does not start a new native poll for a retained demand, and the stream ends`() =
        runTest {
            val row = "F2 receiver/close-parked"
            val rig = rxRig()
            val slot = CompletableDeferred<Result<MediaObject?>>()
            val consumer = launch(Dispatchers.Default) { slot.complete(runCatching { rig.receiver.objects.next() }) }
            assertTrue(rig.inner.awaitCondition("the demand answered Empty") { it.objectPolls.size == 1 }, "$row: the demand never polled")
            assertTrue(rig.inner.awaitParked("the demand is retained and the worker parked"), "$row: no park after retention")
            rig.inner.scriptObjects(DATA) // now available natively, but nothing marks activity: only close's wake follows
            rig.endpoint.close()
            val r = withTimeout(BOUND) { slot.await() }
            assertEquals(null, r.getOrNull(), "$row: a retained demand at close ends the stream; got $r")
            assertTrue(r.isSuccess, "$row: the stream's documented end, never an internal engine exception; got $r")
            withTimeout(BOUND) { consumer.join() }
            withTimeout(BOUND) { rig.endpoint.closeAndJoin() }
            assertEquals(1, rig.inner.observed().objectPolls.size, "$row: close's own wake did not start a new native poll")
            assertEquals(1, rig.inner.observed().objectsLeftQueued, "$row: the object stays native and dies with the C receiver")
        }

    @Test
    fun `receiver -- an object already staged before close is still delivered, then the stream ends`() =
        runTest {
            val row = "F2 receiver/staged-before-close"
            val rig = rxRig()
            rig.inner.scriptObjects(DATA)
            rig.inner.holdNextObjectPoll()
            val cancelled = async(Dispatchers.Default) { rig.receiver.objects.next() }
            try {
                assertTrue(rig.inner.awaitCondition("the poll is held") { it.objectPollHeld }, "$row: the poll was never held")
                cancelled.cancel()
            } finally {
                rig.inner.releaseObjectPoll()
            }
            runCatching { withTimeout(BOUND) { cancelled.await() } }
            assertTrue(rig.inner.awaitCondition("the object is staged") { it.objectsLeftQueued == 0 }, "$row: the held poll did not dequeue")
            rig.endpoint.close()
            val o = withTimeout(BOUND) { rig.receiver.objects.next() }
            assertTrue(o != null && o.track === rig.track && o.payload == DATA.payload, "$row: the staged object is delivered before the end; got $o")
            assertEquals(null, withTimeout(BOUND) { rig.receiver.objects.next() }, "$row: then the end")
            withTimeout(BOUND) { rig.endpoint.closeAndJoin() }
            assertEquals(1, rig.inner.observed().objectPolls.size, "$row: no poll after close")
        }
}
