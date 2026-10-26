package org.openmoq.moq5.engine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.SendBackpressure
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderClosedException
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.TrackName
import org.openmoq.moq5.WriteOutcome
import java.util.concurrent.locks.ReentrantLock
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Arbitration of a retained attempt's CLAIM against shutdown and quarantine,
 * at the engine level (the retained entry is engine-internal).
 *
 *  * Claim versus close. The decision "may this retained attempt start" and
 *    the claim itself are one atomic step against `requestClose()`. The rows
 *    contest that exact interval and classify it by an OBSERVED ORDERING
 *    FACT, never by a deadline: while the entry's own settlement lock is
 *    held, the worker is seen queued on that lock, and then either the
 *    closer RETURNED (it reached and left the engine lock first: close won)
 *    or the closer's exact thread is seen QUEUED ON THE ENGINE LOCK the
 *    claiming worker holds (the claim won). A caller that is merely
 *    unscheduled is neither, and the row fails by name rather than guessing;
 *    the ceiling is an outer safety bound only. The closer runs on an owned
 *    helper and is joined only after the entry lock has been released.
 *  * Claim versus quarantine. The claim consults the CURRENT bridge failure,
 *    so a post-drain record that refuses the token, or a retry that
 *    quarantined the root earlier in the same pass, makes every later
 *    backend call illegal. The inventories below are compared UNFILTERED
 *    after the refusal, against a separately declared legitimate prefix, and
 *    only after a positive barrier: the owned engine's retirement has been
 *    joined, so no later call of the same pass can escape the observation.
 *
 * The lock reads are read-only inspections of the real locks; nothing is
 * written through them and no production seam is added for the test.
 */
internal class EndpointEngineRetainedArbitrationTest : EngineFixture() {
    private companion object {
        val BOUND = 5.seconds
        val TX = SenderConfig(namespace = Namespace(listOf(NamespacePart("live".toByteArray()))), backpressure = SendBackpressure.ReturnWouldBlock)
        val TRACK =
            SendTrackConfig(
                name = TrackName("v".toByteArray()), mediaType = MediaType.Video, packaging = PackagingKind.Raw,
                codec = MediaBytes("avc1.64001f".toByteArray()), bitrate = 2_000_000uL,
            )
        val OBJ = SendObject(payload = MediaBytes("k".toByteArray()), isSync = true, startsGroup = true, presentationTime = MediaMicros(40_000uL))
    }

    private enum class Op { Write, EndTrack }

    private class Rig(val engine: EndpointEngine, val backend: ScriptedEndpointBackend, val sender: AttachedSender, val track: org.openmoq.moq5.SendTrack)

    private suspend fun rig(publishSnapshots: Boolean = false): Rig {
        val backend = newBackend()
        val (engine, _) = newEngine(backend, publishSnapshots) { backend }
        engine.awaitConstructed()
        val sender = engine.attachSender(TX)
        val track = engine.senderAddTrack(sender.token, sender.model, TRACK)
        return Rig(engine, backend, sender, track)
    }

    private fun Rig.setDefault(op: Op, r: BackendWriteResult) {
        when (op) {
            Op.Write -> backend.senderWriteDefault = r
            Op.EndTrack -> backend.senderEndTrackDefault = r
        }
    }

    private fun Rig.attempts(op: Op): Int =
        when (op) {
            Op.Write -> backend.observed().senderWrites.size
            Op.EndTrack -> backend.observed().senderEndTracks.size
        }

    private fun Rig.transferred(op: Op): Int =
        when (op) {
            Op.Write -> backend.observed().acceptedPayloads.size
            Op.EndTrack -> if (track.isEnded) 1 else 0
        }

    private suspend fun Rig.retain(op: Op): Retained<WriteAttempt> {
        val d =
            when (op) {
                Op.Write -> engine.senderWrite(sender.token, sender.model, track, OBJ, retain = true)
                Op.EndTrack -> engine.senderEndTrack(sender.token, sender.model, track, retain = true)
            }
        check(d is Demanded.Pending) { "the first attempt must be refused and retained" }
        return d.retained
    }

    private fun ScriptedEndpointBackend.awaitParked(what: String): Boolean = awaitCondition(what) { it.waitParked && !it.activityLevel }

    /** Join the owned engine's worker: a positive barrier, after which no call of the contested pass can still arrive. */
    private fun Rig.joinRetirement(row: String, expectQuarantine: Boolean) {
        if (expectQuarantine) markQuarantined(engine)
        val closer = callBlocking("$row-retire") { engine.close() }
        val outcome = closer.consume()
        if (expectQuarantine) {
            assertTrue(outcome is CallerOutcome.Threw && outcome.error is BridgeContractException, "$row: retirement reports the quarantine; got $outcome")
        } else {
            assertTrue(outcome is CallerOutcome.NormalReturn, "$row: retirement completed; got $outcome")
        }
        backend.observed().serviceThread?.let { joinBounded(it, "$row: the owned service worker") }
    }

    /* -- the ordering oracle ------------------------------------------------- */

    /**
     * Spin, bounded, until [fact] holds. Used for facts the scripted
     * backend never signals -- a thread queued on a lock -- so waiting on
     * the backend's condition would only re-read them when something
     * unrelated happened to signal it. The bound produces a named failure,
     * never a verdict.
     */
    private fun awaitFact(ceilingMillis: Long = 2_000, fact: () -> Boolean): Boolean {
        val deadline = System.nanoTime() + ceilingMillis * 1_000_000
        while (System.nanoTime() < deadline) {
            if (fact()) return true
            Thread.onSpinWait()
        }
        return fact()
    }

    /** What the observation ESTABLISHED. Unknown is not a winner. */
    private sealed interface ClaimOrder {
        data object CloseFirst : ClaimOrder

        data object ClaimFirst : ClaimOrder

        data object Unknown : ClaimOrder
    }

    /**
     * Observe which of the claim and the close reached the engine lock
     * first, from facts only: the closer RETURNED (it held and released the
     * engine lock), or the closer's exact thread is QUEUED on the engine
     * lock the claiming worker holds. A closer that has not been scheduled
     * satisfies neither and yields [ClaimOrder.Unknown]; the ceiling only
     * bounds how long the fact is waited for.
     */
    private fun observeOrder(engineLock: ReentrantLock, closer: BlockingCaller, ceilingMillis: Long = 2_000): ClaimOrder {
        val deadline = System.nanoTime() + ceilingMillis * 1_000_000
        while (System.nanoTime() < deadline) {
            if (closer.completedNormally()) return ClaimOrder.CloseFirst
            if (engineLock.hasQueuedThread(closer.ownThread())) return ClaimOrder.ClaimFirst
            Thread.onSpinWait()
        }
        return ClaimOrder.Unknown
    }

    /** The retained entry's own settlement lock, and the engine's own lock: read only, to hold and to observe queueing. */
    private fun entryLockOf(entry: Retained<*>): ReentrantLock {
        val field = entry.javaClass.getDeclaredField("lock")
        field.isAccessible = true
        return field.get(entry) as ReentrantLock
    }

    private fun engineLockOf(engine: EndpointEngine): ReentrantLock {
        val field = EndpointEngine::class.java.getDeclaredField("lock")
        field.isAccessible = true
        return field.get(engine) as ReentrantLock
    }

    @Test
    fun `the ordering oracle answers Unknown for a caller that never reached the engine lock, never claim-won`() {
        // A caller that is simply not at the engine is neither winner: "did
        // not return within the bound" must never be read as claim-won.
        val unrelated = ReentrantLock()
        val hold = gate("ordering-control")
        val caller = callBlocking("ordering-control-caller") { hold.await() }
        val order = observeOrder(unrelated, caller, ceilingMillis = 300)
        assertTrue(order is ClaimOrder.Unknown, "an unscheduled or parked caller establishes no ordering; got $order")
        hold.open()
        assertTrue(caller.consume() is CallerOutcome.NormalReturn, "the control's caller returned")
    }

    // -- F1: claim versus close -------------------------------------------------------

    private fun claimVersusClose(op: Op) =
        runBlocking {
            val row = "F1 ${op.name.lowercase()}/claim-vs-close"
            val rig = rig()
            rig.setDefault(op, BackendWriteResult.WouldBlock)
            val entry = rig.retain(op)
            assertTrue(rig.backend.awaitParked("retained and parked"), "$row: no park after retention")
            assertEquals(1, rig.attempts(op), "$row: the initial refused attempt")
            val worker = rig.backend.observed().serviceThread!!
            val entryLock = entryLockOf(entry)
            val engineLock = engineLockOf(rig.engine)
            val closer: BlockingCaller
            val order: ClaimOrder
            entryLock.lock()
            try {
                rig.setDefault(op, BackendWriteResult.Accepted)
                rig.backend.markActivity()
                // Fact 1: the worker has reached the claim and is queued on the entry lock we hold.
                assertTrue(awaitFact { entryLock.hasQueuedThread(worker) }, "$row: the worker never reached the unclaimed entry")
                assertEquals(1, rig.attempts(op), "$row: no attempt before the claim")
                closer = callBlocking("$row-closer") { rig.engine.requestClose() }
                // Fact 2: the closer returned (it reached the engine lock
                // first), or it is queued on the engine lock the claiming
                // worker holds. Nothing is inferred from silence.
                order = observeOrder(engineLock, closer)
            } finally {
                entryLock.unlock() // released BEFORE any join, so the closer can finish
            }
            assertTrue(closer.consume() is CallerOutcome.NormalReturn, "$row: the close request completed")
            val result = runCatching { withTimeout(BOUND) { entry.result.await() } }
            when (order) {
                is ClaimOrder.CloseFirst -> {
                    assertEquals(1, rig.attempts(op), "$row: close reached the engine lock before the claim, yet the later claim still made a native call; result=$result")
                    assertEquals(0, rig.transferred(op), "$row: nothing transferred after close won the claim")
                    assertTrue(result.exceptionOrNull() is SenderClosedException, "$row: close winning the unclaimed entry answers with the sender-close outcome; got $result")
                }
                is ClaimOrder.ClaimFirst -> {
                    assertEquals(2, rig.attempts(op), "$row: the worker held the engine lock across its claim, so the attempt ran and settles under the in-flight rule")
                    assertEquals(WriteOutcome.Accepted, result.getOrNull()?.outcome, "$row: the claimed attempt's transfer is delivered; got $result")
                    assertEquals(1, rig.transferred(op), "$row: exactly one transfer")
                }
                is ClaimOrder.Unknown ->
                    throw AssertionError("$row: neither ordering fact could be established while the entry lock was held -- the closer neither returned nor queued on the engine lock; the row refuses to guess a winner")
            }
            val expected = rig.attempts(op)
            rig.joinRetirement(row, expectQuarantine = false)
            assertEquals(expected, rig.attempts(op), "$row: nothing after teardown")
        }

    @Test
    fun `write -- close winning the unclaimed entry permits no native call, a claim winning settles in flight`() = claimVersusClose(Op.Write)

    @Test
    fun `endTrack -- close winning the unclaimed entry permits no native call, a claim winning settles in flight`() = claimVersusClose(Op.EndTrack)

    // -- F2/F3: claim versus quarantine, with complete unfiltered inventories -----------

    /**
     * The post-drain observation, with its EARLIER pass declared and settled
     * first.
     *
     * `runDrain` wakes the worker and then waits only for `workerOffWait`,
     * which the worker sets the moment its backend wait returns -- before its
     * probe, its retained retry and its record for that same wake. So a fault
     * armed on "the native drain is in flight" can be consumed by that earlier
     * ordinary pass instead of the post-drain observation, on either side of
     * the drain record (the backend records "drain" on RETURN).
     *
     * Both arms therefore hold the drain's RETURN until the ordinary pass has
     * reached a COMPLETED record, declare that pass's records separately, and
     * only then arm the refusal for the post-drain observation. What differs
     * between them is where the drain is stopped meanwhile -- ENTERED and in
     * flight, or holding a granted turn it has not taken up -- not the
     * ordinary pass's ordering against the drain's return, which is fixed the
     * same way in both. In both, the tail after arming is compared UNFILTERED
     * and in full: the post-drain branch probes, records the refusal, and makes
     * no further backend call of any kind, though the retained write would now
     * be accepted if one were made.
     */
    private fun postDrainQuarantine(drainEnteredFirst: Boolean) =
        runBlocking {
            val row =
                if (drainEnteredFirst) "F2 post-drain-quarantine (the drain entered its row first)"
                else "F2 post-drain-quarantine (the drain held a granted turn it had not taken up)"
            val rig = rig(publishSnapshots = true)
            rig.backend.senderWriteDefault = BackendWriteResult.WouldBlock
            val entry = rig.retain(Op.Write)
            assertTrue(rig.backend.awaitParked("retained before the drain"), "$row: no park after retention")
            val baseline = rig.backend.observed().callOrder.size

            val failure = BridgeFailure("post-drain-snapshot", -12346)
            var armed = 0
            var releasedDrain = false
            var releasedProbe = false
            // Both arms stop the native drain from RETURNING while the earlier
            // pass runs, and differ in where it is stopped: inside the row it
            // has entered, or before it, holding a granted turn it has not yet
            // taken up. Either way the earlier pass is driven to a COMPLETED
            // record before anything is armed.
            if (drainEnteredFirst) rig.backend.holdDrain() else rig.backend.holdNextDrainEntry()
            val drainer = async(Dispatchers.Default) { rig.engine.drain(1_000_000L) }
            // The primary failure is kept and rethrown: cleanup runs whatever
            // happens, and a cleanup failure is reported alongside it rather
            // than replacing it.
            var primary: Throwable? = null
            try {
                if (drainEnteredFirst) {
                    assertTrue(rig.backend.awaitCondition("the native drain is in flight") { it.drainInFlight }, "$row: the drain never entered")
                } else {
                    assertTrue(rig.backend.awaitCondition("the granted turn has not been taken up") { it.drainEntryHeld }, "$row: the drain never reached its entry")
                }
                assertTrue(
                    rig.backend.awaitCondition("the earlier pass recorded and returned") { it.callOrder.drop(baseline).contains("snapshot") },
                    "$row: the earlier pass never completed its own record; order=${rig.backend.observed().callOrder}",
                )
                assertEquals(
                    listOf("sender-probe", "sender-write", "snapshot"),
                    rig.backend.observed().callOrder.drop(baseline),
                    "$row: the earlier pass and its refused retry, declared: one probe, the WouldBlock retry, and that pass's own normal record",
                )
                // The post-drain observation's own first call is gated BEFORE
                // the drain is allowed to return, so nothing can be armed for
                // the wrong pass.
                rig.backend.holdNextSenderProbe()

                // Everything before this point is the earlier pass. From here
                // the only backend calls that can happen are the post-drain
                // observation's, and an accepted write is available to any call
                // the quarantine fails to prevent.
                armed = rig.backend.observed().callOrder.size
                rig.backend.snapshotResult = BackendSnapshotResult.Bridge(failure)
                rig.backend.senderWriteDefault = BackendWriteResult.Accepted

                if (drainEnteredFirst) rig.backend.releaseDrain() else rig.backend.releaseDrainEntry()
                releasedDrain = true
                assertTrue(
                    rig.backend.awaitCondition("the post-drain observation has begun") { it.senderProbeHeld },
                    "$row: the post-drain branch was never reached; order=${rig.backend.observed().callOrder}",
                )
                rig.backend.releaseSenderProbe()
                releasedProbe = true
                withTimeout(BOUND) { drainer.await() }
                val result = runCatching { withTimeout(BOUND) { entry.result.await() } }
                assertTrue(result.exceptionOrNull() is BridgeContractException, "$row: the pending write learns the quarantine; got $result")
                assertEquals(failure, (result.exceptionOrNull() as BridgeContractException).failure, "$row: the same stable failure")
            } catch (t: Throwable) {
                primary = t
            }
            val cleanup =
                runCatching {
                    if (!releasedDrain) {
                        if (drainEnteredFirst) rig.backend.releaseDrain() else rig.backend.releaseDrainEntry()
                    }
                    if (!releasedProbe) rig.backend.releaseSenderProbe()
                    // The positive barrier: the owned worker has retired, so no
                    // call of the contested pass can still arrive. A quarantined
                    // retirement itself makes no backend call, so it cannot
                    // manufacture the empty tail asserted below.
                    rig.joinRetirement(row, expectQuarantine = true)
                }.exceptionOrNull()
            primary?.let { p ->
                cleanup?.let { p.addSuppressed(it) }
                throw p
            }
            cleanup?.let { throw it }
            val s = rig.backend.observed()
            assertEquals(0, s.stopCount, "$row: a quarantined retirement stops nothing")
            assertEquals(0, s.destroyCount, "$row: and destroys nothing, so the tail below is the pass's own")
            assertEquals(
                listOf("drain", "sender-probe", "snapshot"), s.callOrder.drop(armed),
                "$row: UNFILTERED -- the post-drain observation probes, records the refusal, and makes no backend call of any kind afterwards; whole order was ${s.callOrder}",
            )
            assertEquals(0, s.acceptedPayloads.size, "$row: nothing transferred")
        }

    @Test
    fun `a post-drain record that refuses the token quarantines before retained work, and its whole post-refusal tail is the observation itself`() =
        postDrainQuarantine(drainEnteredFirst = true)

    @Test
    fun `the same holds when the drain had only been granted its turn while the earlier pass ran`() =
        postDrainQuarantine(drainEnteredFirst = false)

    @Test
    fun `a retry that quarantines the root ends that pass -- the whole post-refusal suffix is empty and every waiter gets the one failure`() =
        runBlocking {
            val row = "F2 pass-suffix-quarantine"
            val rig = rig(publishSnapshots = true)
            rig.backend.senderWriteDefault = BackendWriteResult.WouldBlock
            val a = rig.retain(Op.Write)
            val b = rig.retain(Op.Write)
            assertTrue(rig.backend.awaitParked("both retained at an empty park"), "$row: no park after retention")
            val before = rig.backend.observed()
            val failure = BridgeFailure("retained-write", -12345)
            rig.backend.scriptSenderWrite(BackendWriteResult.Bridge(failure), BackendWriteResult.Accepted)
            rig.backend.markActivity()
            try {
                val ar = runCatching { withTimeout(BOUND) { a.result.await() } }
                val br = runCatching { withTimeout(BOUND) { b.result.await() } }
                assertTrue(ar.exceptionOrNull() is BridgeContractException, "$row: the first entry reports the bridge failure: $ar")
                assertEquals(failure, (ar.exceptionOrNull() as BridgeContractException).failure)
                assertSame(ar.exceptionOrNull(), br.exceptionOrNull(), "$row: both waiters learn the one stable failure")
            } finally {
                // Result publication can precede the rest of the worker's
                // pass, so the inventory is read only after the retirement
                // barrier below.
                rig.joinRetirement(row, expectQuarantine = true)
            }
            val s = rig.backend.observed()
            assertEquals(0, s.stopCount, "$row: a quarantined retirement stops nothing")
            assertEquals(0, s.destroyCount, "$row: and destroys nothing")
            val newCalls = s.callOrder.drop(before.callOrder.size)
            val refusal = newCalls.indexOf("sender-write")
            assertTrue(refusal >= 0, "$row: the refusing write is in this pass: $newCalls")
            assertEquals(listOf("sender-probe"), newCalls.take(refusal), "$row: the declared legitimate pre-refusal prefix, unfiltered; saw $newCalls")
            assertEquals(emptyList(), newCalls.drop(refusal + 1), "$row: UNFILTERED -- after the refusing write the pass makes no further backend call: no retained retry, no probe, no record; saw $newCalls")
            assertEquals(before.senderWrites.size + 1, s.senderWrites.size, "$row: exactly one native write in the quarantined pass")
            assertEquals(0, s.acceptedPayloads.size, "$row: nothing transferred")
        }
}
