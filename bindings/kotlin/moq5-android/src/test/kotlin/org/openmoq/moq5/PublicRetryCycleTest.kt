package org.openmoq.moq5

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendObjectResult
import org.openmoq.moq5.engine.BackendSapRecord
import org.openmoq.moq5.engine.BackendTimelineRecord
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendWaitResult
import org.openmoq.moq5.engine.BackendWriteResult
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.EngineLedger
import org.openmoq.moq5.engine.Gate
import org.openmoq.moq5.engine.HookedBackend
import org.openmoq.moq5.engine.ManualDispatcher
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.Continuation
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.startCoroutine
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * No self-generated retry cycle, and no lost activity, RED-first.
 *
 * Source-derived: a consumer's demand (`objects`, `sapRecords`,
 * `mediaTimeline`) and a sender's pass-driven retry (`write`, `endTrack`
 * under `ReturnWouldBlock`) each submit one engine job, and every accepted
 * submission publishes the backend's coalesced, level-retained wake. When
 * the worker is NOT inside its backend wait at that moment -- it is running
 * a held job, is parked for a native drain, or is resuming this very
 * consumer inline from the pass it just completed -- `claimNext` claims the
 * job before that wake was consumed. The job answers Empty / WouldBlock with
 * the CURRENT pass count, the consumer waits for the pass after it, and the
 * worker's next backend wait returns ACTIVITY at once on the consumer's own
 * submission wake: a pass with no data, a released consumer, a retry, another
 * wake. With inline delivery (a bare continuation, no dispatcher: the
 * reentry suite's pattern) the cycle is deterministic and unbounded.
 *
 * Two kinds of rows, judged separately:
 *  * QUIESCENCE rows require POSITIVE evidence of a parked wait: the backend
 *    reports a genuinely parked wait with NO pending activity level, both
 *    observed atomically under its own lock -- never elapsed silence, never
 *    a chosen poll count. One confirmation attempt from an already issued
 *    wake is legitimate and is stated where it applies; the drain row's
 *    bound is explained for its geometry only. Then ONE genuine activity
 *    carrying data or capacity must progress the consumer with exactly one
 *    more native attempt.
 *  * LOST-ACTIVITY rows admit data and activity in the windows a quiescence
 *    fix could get wrong -- while the poll's actual Empty answer is held
 *    before it returns, with the submission wake still pending so the
 *    genuine activity coalesces into it, and while a native drain that
 *    consumes the level (as the C drain's own inner wait does) is in flight
 *    -- and require delivery WITHOUT another external nudge, judged by the
 *    delivery and the recorded consumption order, not by any attempt bound.
 */
internal class PublicRetryCycleTest : EngineFixture() {
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
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)

        /* The declared shapes: each feeds both the script and the expectation.
         * DATA is a legal RAW Normal object (`media_object.c` RAW path): a
         * payload, composition offset zero, and a present capture timestamp
         * driving decode and presentation time; endOfGroup is the separate flag. */
        val CAPTURE = UnixMicros(1_700_000_000_000_009uL)
        val DATA =
            BackendObject(
                track = T1, payload = MediaPayload.Raw(MediaBytes("data".toByteArray())), status = ObjectStatus.Normal, endOfGroup = true,
                viaDatagram = false, keyframe = true, captureTime = CAPTURE,
                decodeTime = MediaMicros(CAPTURE.value), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(CAPTURE.value),
            )
        val SAP1 = BackendSapRecord(track = T1, location = ObjectLocation(7uL, 1uL), sapType = SapType.Type2, earliestPresentationTime = MediaMillis(10uL))
        val TL3 = BackendTimelineRecord(track = T1, mediaTime = MediaMillis(120uL), location = ObjectLocation(7uL, 3uL), wallclock = UnixMillis.fromNativeOrNull(1_700_000_000_003uL))
        val OBJ =
            SendObject(
                payload = MediaBytes("k".toByteArray()), isSync = true, startsGroup = true, presentationTime = MediaMicros(40_000uL),
                endsGroup = false, decodeTime = MediaMicros(39_000uL), captureTime = UnixMicros(1_700_000_000_000_040uL), sapType = SapType.Type1, cmafProperties = null,
            )
    }

    private class RxRig(val inner: ScriptedEndpointBackend, val hooked: HookedBackend, val endpoint: MoqEndpoint, val receiver: MediaReceiver, val track: MediaTrack)

    private class TxRig(val inner: ScriptedEndpointBackend, val hooked: HookedBackend, val endpoint: MoqEndpoint, val sender: MediaSender, val track: SendTrack)

    private class Held(val gate: Gate, val job: Deferred<*>)

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun rxRig(): RxRig {
        val inner = newBackend()
        val hooked = HookedBackend(inner)
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { hooked }, inner)
        val receiver = endpoint.attachReceiver(RX)
        inner.scriptTrackEvents(added(T1, VIDEO_RX), ready)
        inner.markActivity()
        val track = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }.tracks.single()
        return RxRig(inner, hooked, endpoint, receiver, track)
    }

    private suspend fun txRig(): TxRig {
        val inner = newBackend()
        val hooked = HookedBackend(inner)
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { hooked }, inner)
        val sender = endpoint.attachSender(TX)
        val track = withTimeout(BOUND) { sender.addTrack(VIDEO_TX) }
        return TxRig(inner, hooked, endpoint, sender, track)
    }

    /** Occupy the worker with a stats read whose native call is held by a gate, so the next submission is provably queued and its wake unconsumed. */
    private suspend fun CoroutineScope.holdReceiverWorker(rig: RxRig, ledger: EngineLedger): Held {
        val hold = gate("hold")
        rig.hooked.beforeReceiverStats = { ledger.started("held"); hold.await() }
        val job = async(Dispatchers.Default) { rig.receiver.statsSnapshot() }
        assertTrue(ledger.await("the preceding job is claimed") { it.started.contains("held") }, "R5: the holding job never ran")
        return Held(hold, job)
    }

    private suspend fun CoroutineScope.holdSenderWorker(rig: TxRig, ledger: EngineLedger): Held {
        val hold = gate("hold")
        rig.hooked.beforeSenderStats = { ledger.started("held"); hold.await() }
        val job = async(Dispatchers.Default) { rig.sender.statsSnapshot() }
        assertTrue(ledger.await("the preceding job is claimed") { it.started.contains("held") }, "R5: the holding job never ran")
        return Held(hold, job)
    }

    /** Start [body] as a bare coroutine with NO dispatcher: every resumption the worker performs continues it inline on the worker. */
    private fun <T> startInline(done: Gate, body: suspend () -> T): AtomicReference<Result<T>?> {
        val outcome = AtomicReference<Result<T>?>(null)
        body.startCoroutine(
            Continuation(EmptyCoroutineContext) { r ->
                outcome.set(r)
                done.open()
            },
        )
        return outcome
    }

    /** Positive evidence: the worker is inside a genuinely parked wait and no activity level is pending, both under the backend's lock. */
    private fun ScriptedEndpointBackend.awaitParked(what: String): Boolean = awaitCondition(what) { it.waitParked && !it.activityLevel }

    private fun cycle(s: ScriptedEndpointBackend.Snapshot, attempts: Int): String =
        "$attempts native attempts, ${s.waitReturns.size} wait returns of which ${s.waitReturns.count { it == BackendWaitResult.ACTIVITY }} ACTIVITY, ${s.wakeCount} wakes"

    private fun assertData(row: String, o: MediaObject?, track: MediaTrack) {
        assertTrue(o != null, "$row: the genuine object was not delivered (got null)")
        assertTrue(o.track === track, "$row: the delivered object names the inventory's track instance")
        assertEquals(DATA.payload, o.payload, "$row: payload")
        assertEquals(DATA.status, o.status, "$row: status")
        assertEquals(DATA.endOfGroup, o.endOfGroup, "$row: endOfGroup")
        assertEquals(DATA.viaDatagram, o.viaDatagram, "$row: viaDatagram")
        assertEquals(DATA.keyframe, o.keyframe, "$row: keyframe")
        assertEquals(DATA.captureTime, o.captureTime, "$row: captureTime")
        assertEquals(DATA.decodeTime, o.decodeTime, "$row: decodeTime")
        assertEquals(DATA.compositionOffset, o.compositionOffset, "$row: compositionOffset")
        assertEquals(DATA.presentationTime, o.presentationTime, "$row: presentationTime")
    }

    /** Every sender attempt carried the original target track and the complete declared object. */
    private fun assertAttemptsCarry(row: String, rig: TxRig) {
        val writes = rig.inner.observed().senderWrites
        assertTrue(writes.isNotEmpty(), "$row: at least one attempt")
        assertTrue(writes.all { it.track == rig.track.token && it.value == OBJ && it.token == rig.inner.observed().senderAttaches.single().token }, "$row: every attempt carries the sender, the target track and the identical complete SendObject: $writes")
    }

    // -- quiescence: the receiver's demand streams --------------------------------

    @Test
    fun `objects -- an Empty demand behind a held job parks the worker instead of feeding its own wake back as a retry, and genuine data progresses it`() =
        runTest {
            val rig = rxRig()
            val ledger = ledger()
            val held = holdReceiverWorker(rig, ledger)
            val wakes = rig.inner.observed().wakeCount
            val done = gate("objects-done")
            val outcome = startInline(done) { rig.receiver.objects.next() }
            assertTrue(rig.inner.awaitCondition("the demand is accepted") { it.wakeCount == wakes + 1 }, "R5 objects: the demand was never accepted")
            held.gate.open()
            withTimeout(BOUND) { held.job.await() }
            val parked = rig.inner.awaitParked("the worker parks with nothing pending after the empty demand")
            val s = rig.inner.observed()
            assertTrue(parked, "R5 objects: persistent Empty answers never let the worker park: ${cycle(s, s.objectPolls.size)} -- each retry consumed its own submission wake and resubmitted")
            assertTrue(s.objectPolls.size <= 2, "R5 objects: the demand plus at most one confirmation from the already issued wake; saw ${s.objectPolls.size} polls")
            assertEquals(null, outcome.get(), "R5 objects: nothing is delivered without data")
            rig.inner.scriptObjects(DATA)
            rig.inner.markActivity()
            assertTrue(done.await(), "R5 objects: one genuine activity with data did not progress the consumer")
            assertData("R5 objects", outcome.get()!!.getOrThrow(), rig.track)
            assertEquals(s.objectPolls.size + 1, rig.inner.observed().objectPolls.size, "R5 objects: exactly one more poll for the genuine activity")
        }

    @Test
    fun `SAP records -- an Empty demand behind a held job parks the worker, and a genuine record progresses it`() =
        runTest {
            val rig = rxRig()
            val ledger = ledger()
            val held = holdReceiverWorker(rig, ledger)
            val wakes = rig.inner.observed().wakeCount
            val done = gate("sap-done")
            val outcome = startInline(done) { rig.receiver.sapRecords.next() }
            assertTrue(rig.inner.awaitCondition("the demand is accepted") { it.wakeCount == wakes + 1 }, "R5 sap: the demand was never accepted")
            held.gate.open()
            withTimeout(BOUND) { held.job.await() }
            val parked = rig.inner.awaitParked("the worker parks with nothing pending after the empty SAP demand")
            val s = rig.inner.observed()
            assertTrue(parked, "R5 sap: persistent Empty answers never let the worker park: ${cycle(s, s.sapPolls.size)}")
            assertTrue(s.sapPolls.size <= 2, "R5 sap: the demand plus at most one confirmation; saw ${s.sapPolls.size} polls")
            assertEquals(null, outcome.get(), "R5 sap: nothing is delivered without a record")
            rig.inner.scriptSapRecords(SAP1)
            rig.inner.markActivity()
            assertTrue(done.await(), "R5 sap: one genuine activity with a record did not progress the consumer")
            val rec = (outcome.get()!!.getOrThrow() as? ReceiverRecordEvent.Record<*>)?.value as? SapRecord
            assertTrue(rec != null && rec.track === rig.track && rec.location == SAP1.location && rec.sapType == SAP1.sapType && rec.earliestPresentationTime == SAP1.earliestPresentationTime, "R5 sap: the genuine record is delivered with its complete shape; got ${outcome.get()}")
            assertEquals(s.sapPolls.size + 1, rig.inner.observed().sapPolls.size, "R5 sap: exactly one more poll for the genuine activity")
        }

    @Test
    fun `media timeline -- an Empty demand behind a held job parks the worker, and a genuine record progresses it`() =
        runTest {
            val rig = rxRig()
            val ledger = ledger()
            val held = holdReceiverWorker(rig, ledger)
            val wakes = rig.inner.observed().wakeCount
            val done = gate("timeline-done")
            val outcome = startInline(done) { rig.receiver.mediaTimeline.next() }
            assertTrue(rig.inner.awaitCondition("the demand is accepted") { it.wakeCount == wakes + 1 }, "R5 timeline: the demand was never accepted")
            held.gate.open()
            withTimeout(BOUND) { held.job.await() }
            val parked = rig.inner.awaitParked("the worker parks with nothing pending after the empty timeline demand")
            val s = rig.inner.observed()
            assertTrue(parked, "R5 timeline: persistent Empty answers never let the worker park: ${cycle(s, s.timelinePolls.size)}")
            assertTrue(s.timelinePolls.size <= 2, "R5 timeline: the demand plus at most one confirmation; saw ${s.timelinePolls.size} polls")
            assertEquals(null, outcome.get(), "R5 timeline: nothing is delivered without a record")
            rig.inner.scriptTimelineRecords(TL3)
            rig.inner.markActivity()
            assertTrue(done.await(), "R5 timeline: one genuine activity with a record did not progress the consumer")
            val rec = (outcome.get()!!.getOrThrow() as? ReceiverRecordEvent.Record<*>)?.value as? MediaTimelineRecord
            assertTrue(rec != null && rec.track === rig.track && rec.location == TL3.location && rec.mediaTime == TL3.mediaTime && rec.wallclock == TL3.wallclock, "R5 timeline: the genuine record is delivered with its complete shape; got ${outcome.get()}")
            assertEquals(s.timelinePolls.size + 1, rig.inner.observed().timelinePolls.size, "R5 timeline: exactly one more poll for the genuine activity")
        }

    // -- quiescence: the sender's pass-driven retries ------------------------------

    @Test
    fun `write retry -- persistent WouldBlock behind a held job parks the worker, and genuine capacity accepts the identical object`() =
        runTest {
            val rig = txRig()
            val ledger = ledger()
            rig.inner.senderWriteDefault = BackendWriteResult.WouldBlock
            val held = holdSenderWorker(rig, ledger)
            val wakes = rig.inner.observed().wakeCount
            val done = gate("write-done")
            val outcome = startInline(done) { rig.sender.write(rig.track, OBJ, Duration.INFINITE) }
            assertTrue(rig.inner.awaitCondition("the attempt is accepted") { it.wakeCount == wakes + 1 }, "R5 write: the attempt was never accepted")
            held.gate.open()
            withTimeout(BOUND) { held.job.await() }
            val parked = rig.inner.awaitParked("the worker parks with nothing pending after the WouldBlock attempt")
            val s = rig.inner.observed()
            assertTrue(parked, "R5 write: persistent WouldBlock never let the worker park: ${cycle(s, s.senderWrites.size)} -- each retry consumed its own submission wake and resubmitted")
            assertTrue(s.senderWrites.size <= 2, "R5 write: the attempt plus at most one confirmation from the already issued wake; saw ${s.senderWrites.size} attempts")
            assertEquals(null, outcome.get(), "R5 write: no outcome without capacity")
            assertEquals(0, s.acceptedPayloads.size, "R5 write: nothing transferred on WouldBlock")
            rig.inner.senderWriteDefault = BackendWriteResult.Accepted
            rig.inner.markActivity()
            assertTrue(done.await(), "R5 write: one genuine activity with capacity did not progress the writer")
            assertEquals(WriteOutcome.Accepted, outcome.get()!!.getOrThrow(), "R5 write: accepted on the genuine pass")
            assertEquals(listOf("k"), rig.inner.observed().acceptedPayloads.map { String(it) }, "R5 write: the identical object crossed once")
            assertEquals(s.senderWrites.size + 1, rig.inner.observed().senderWrites.size, "R5 write: exactly one more attempt for the genuine activity")
            assertAttemptsCarry("R5 write", rig)
        }

    @Test
    fun `endTrack retry -- persistent WouldBlock behind a held job parks the worker, and genuine capacity accepts the marker`() =
        runTest {
            val rig = txRig()
            val ledger = ledger()
            rig.inner.senderEndTrackDefault = BackendWriteResult.WouldBlock
            val held = holdSenderWorker(rig, ledger)
            val wakes = rig.inner.observed().wakeCount
            val done = gate("end-done")
            val outcome = startInline(done) { rig.sender.endTrack(rig.track, Duration.INFINITE) }
            assertTrue(rig.inner.awaitCondition("the attempt is accepted") { it.wakeCount == wakes + 1 }, "R5 endTrack: the attempt was never accepted")
            held.gate.open()
            withTimeout(BOUND) { held.job.await() }
            val parked = rig.inner.awaitParked("the worker parks with nothing pending after the WouldBlock end-track attempt")
            val s = rig.inner.observed()
            assertTrue(parked, "R5 endTrack: persistent WouldBlock never let the worker park: ${cycle(s, s.senderEndTracks.size)}")
            assertTrue(s.senderEndTracks.size <= 2, "R5 endTrack: the attempt plus at most one confirmation; saw ${s.senderEndTracks.size} attempts")
            assertEquals(null, outcome.get(), "R5 endTrack: no outcome without capacity")
            assertTrue(!rig.track.isEnded, "R5 endTrack: nothing latched on WouldBlock")
            rig.inner.senderEndTrackDefault = BackendWriteResult.Accepted
            rig.inner.markActivity()
            assertTrue(done.await(), "R5 endTrack: one genuine activity with capacity did not progress the caller")
            assertEquals(WriteOutcome.Accepted, outcome.get()!!.getOrThrow(), "R5 endTrack: accepted on the genuine pass")
            assertTrue(rig.track.isEnded, "R5 endTrack: the accepted marker is remembered on the handle")
            assertEquals(s.senderEndTracks.size + 1, rig.inner.observed().senderEndTracks.size, "R5 endTrack: exactly one more attempt for the genuine activity")
            assertTrue(rig.inner.observed().senderEndTracks.all { it.track == rig.track.token }, "R5 endTrack: every attempt targets the original track")
        }

    // -- quiescence: coalescing and timing variants (no data) ----------------------

    @Test
    fun `activity coalesced with the initial wake -- one level, one pass, then a park with no self-retry`() =
        runTest {
            val rig = rxRig()
            val ledger = ledger()
            val held = holdReceiverWorker(rig, ledger)
            // A genuine activity that carries no object lands first; the
            // demand's submission wake then coalesces into the same level.
            rig.inner.markActivity()
            val wakes = rig.inner.observed().wakeCount
            val done = gate("coalesced-done")
            val outcome = startInline(done) { rig.receiver.objects.next() }
            assertTrue(rig.inner.awaitCondition("the demand is accepted") { it.wakeCount == wakes + 1 }, "R5 coalesced: the demand was never accepted")
            held.gate.open()
            withTimeout(BOUND) { held.job.await() }
            val parked = rig.inner.awaitParked("the worker parks after the coalesced level is consumed")
            val s = rig.inner.observed()
            assertTrue(parked, "R5 coalesced: the worker never parked: ${cycle(s, s.objectPolls.size)}")
            assertTrue(s.objectPolls.size <= 2, "R5 coalesced: the demand plus at most one confirmation for the coalesced level; saw ${s.objectPolls.size} polls")
            assertEquals(null, outcome.get(), "R5 coalesced: nothing is delivered without data")
            rig.inner.scriptObjects(DATA)
            rig.inner.markActivity()
            assertTrue(done.await(), "R5 coalesced: genuine data did not progress the consumer")
            assertData("R5 coalesced", outcome.get()!!.getOrThrow(), rig.track)
            assertEquals(s.objectPolls.size + 1, rig.inner.observed().objectPolls.size, "R5 coalesced: exactly one more poll for the genuine activity")
        }

    @Test
    fun `activity between the poll and the park -- exactly one confirmation poll, then a park with no self-retry`() =
        runTest {
            val rig = rxRig()
            rig.inner.holdNextObjectPoll()
            val done = gate("between-done")
            val outcome = startInline(done) { rig.receiver.objects.next() }
            try {
                assertTrue(rig.inner.awaitCondition("the demand's poll is held") { it.objectPollHeld }, "R5 between: the poll was never held")
                // Genuine activity, carrying no object, lands after the poll
                // began and before the consumer could park.
                rig.inner.markActivity()
            } finally {
                rig.inner.releaseObjectPoll()
            }
            val parked = rig.inner.awaitParked("the worker parks after confirming the genuine activity")
            val s = rig.inner.observed()
            assertTrue(parked, "R5 between: the worker never parked: ${cycle(s, s.objectPolls.size)} -- the confirmation's own wake was fed back")
            assertEquals(2, s.objectPolls.size, "R5 between: the held poll and exactly one confirmation for the genuine activity that landed after it")
            assertEquals(null, outcome.get(), "R5 between: nothing is delivered without data")
            rig.inner.scriptObjects(DATA)
            rig.inner.markActivity()
            assertTrue(done.await(), "R5 between: genuine data did not progress the consumer")
            assertData("R5 between", outcome.get()!!.getOrThrow(), rig.track)
            assertEquals(s.objectPolls.size + 1, rig.inner.observed().objectPolls.size, "R5 between: exactly one more poll for the genuine activity")
        }

    @Test
    fun `activity during a native drain -- the demand runs during the drain, and after it the worker parks with no self-retry`() =
        runTest {
            val rig = rxRig()
            var drainHeld = true
            try {
                rig.inner.holdDrain()
                val drainer = async(Dispatchers.Default) { rig.endpoint.drain(1.seconds) }
                assertTrue(rig.inner.awaitCondition("the native drain is in flight") { it.drainInFlight }, "R5 drain: the native drain never started")
                val done = gate("drain-done")
                val outcome = startInline(done) { rig.receiver.objects.next() }
                assertTrue(rig.inner.awaitCondition("the demand ran during the drain, off the backend wait") { it.objectPolls.size == 1 }, "R5 drain: the demand did not run while the worker was parked for the drain")
                rig.inner.releaseDrain()
                drainHeld = false
                assertEquals(DrainOutcome.Flushed, withTimeout(BOUND) { drainer.await() }, "R5 drain: the drain completes")
                val parked = rig.inner.awaitParked("the worker parks after the drain's own observation and the pending level")
                val s = rig.inner.observed()
                assertTrue(parked, "R5 drain: the worker never parked after the drain: ${cycle(s, s.objectPolls.size)}")
                // An explained bound for THIS geometry only (not a retry cap):
                // the demand, the post-drain observation's confirmation, and
                // at most one for the submission wake the drain left
                // unconsumed. Fewer is fine.
                assertTrue(s.objectPolls.size <= 3, "R5 drain: demand + post-drain confirmation + at most one for the retained level; saw ${s.objectPolls.size} polls")
                assertEquals(null, outcome.get(), "R5 drain: nothing is delivered without data")
                rig.inner.scriptObjects(DATA)
                rig.inner.markActivity()
                assertTrue(done.await(), "R5 drain: genuine data did not progress the consumer")
                assertData("R5 drain", outcome.get()!!.getOrThrow(), rig.track)
                assertEquals(s.objectPolls.size + 1, rig.inner.observed().objectPolls.size, "R5 drain: exactly one more poll for the genuine activity")
            } finally {
                if (drainHeld) rig.inner.releaseDrain()
            }
        }

    // -- lost activity: progress controls, judged without an attempt bound ---------

    /**
     * The poll's ACTUAL Empty answer is captured and held before it returns
     * to the engine; data and genuine activity are admitted in that window;
     * then the captured Empty is returned. The consumer must receive the
     * object without another external nudge.
     */
    private suspend fun CoroutineScope.heldEmptyRow(row: String, wakePending: Boolean) {
        val rig = rxRig()
        val ledger = ledger()
        val held = if (wakePending) holdReceiverWorker(rig, ledger) else null
        val emptyHeld = gate("$row-empty-held")
        val captured = AtomicReference<BackendObjectResult?>(null)
        rig.hooked.afterPollObject = { r ->
            if (r is BackendObjectResult.Empty && captured.compareAndSet(null, r)) {
                ledger.started("empty-held")
                emptyHeld.await()
                ledger.completed("empty-held")
            }
        }
        val done = gate("$row-done")
        val outcome = startInline(done) { rig.receiver.objects.next() }
        held?.gate?.open()
        assertTrue(ledger.await("the actual Empty answer is held before it returns") { it.started.contains("empty-held") }, "$row: the poll never answered Empty")
        assertTrue(captured.get() is BackendObjectResult.Empty, "$row: the captured answer is the actual Empty")
        val wakes = rig.inner.observed().wakeCount
        // With the worker held at submission, the demand's own wake is still
        // pending when its Empty answer is held: the level is up BEFORE any
        // genuine activity, so the activity below coalesces into it.
        if (wakePending) assertTrue(rig.inner.observed().activityLevel, "$row: the submission wake is still pending while the Empty answer is held")
        // In the window: data lands and the endpoint marks genuine activity.
        rig.inner.scriptObjects(DATA)
        rig.inner.markActivity()
        emptyHeld.open()
        assertTrue(done.await(), "$row: the object admitted while the Empty answer was held was never delivered without another nudge")
        assertData(row, outcome.get()!!.getOrThrow(), rig.track)
        // No markActivity was issued after the window: the delivery came from
        // the activity admitted while the Empty answer was held. The wakes
        // since the window are the engine's own submissions, never the test's.
        assertTrue(rig.inner.observed().wakeCount >= wakes, "$row: wake accounting is monotonic")
        val polls = rig.inner.observed().objectPolls
        assertEquals(2, polls.size, "$row: the held Empty poll and the one poll that delivered")
        assertTrue(polls[1].result is BackendObjectResult.Object, "$row: the second poll delivered the object")
        held?.let { withTimeout(BOUND) { it.job.await() } }
    }

    @Test
    fun `lost activity -- data and activity admitted while the actual Empty answer is held are delivered without another nudge`() =
        runTest { heldEmptyRow("R5 lost/held-empty", wakePending = false) }

    @Test
    fun `lost activity -- with the submission wake still pending, coalesced genuine activity with data is delivered without another nudge`() =
        runTest { heldEmptyRow("R5 lost/coalesced-into-pending-wake", wakePending = true) }

    @Test
    fun `lost activity -- data arriving during a drain whose own wait consumed the level is delivered without another nudge`() =
        runTest {
            val row = "R5 lost/drain-consumed"
            val rig = rxRig()
            rig.inner.drainConsumesActivity = true
            var drainHeld = true
            try {
                rig.inner.holdDrain()
                val drainer = async(Dispatchers.Default) { rig.endpoint.drain(1.seconds) }
                assertTrue(rig.inner.awaitCondition("the native drain is in flight") { it.drainInFlight }, "$row: the native drain never started")
                val done = gate("$row-done")
                val outcome = startInline(done) { rig.receiver.objects.next() }
                assertTrue(rig.inner.awaitCondition("the initial demand answered Empty during the drain") { it.objectPolls.size == 1 && it.objectPolls[0].result is BackendObjectResult.Empty }, "$row: the initial Empty never happened")
                // Data and activity arrive while the drain is held; on its
                // successful completion the drain's own wait consumes that
                // level, as the C drain's pending loop does before it answers
                // OK. Only that successful path is modelled and judged here.
                rig.inner.scriptObjects(DATA)
                rig.inner.markActivity()
                assertTrue(rig.inner.observed().activityLevel, "$row: the level is raised while the drain owns the endpoint's wait")
                rig.inner.releaseDrain()
                drainHeld = false
                assertEquals(DrainOutcome.Flushed, withTimeout(BOUND) { drainer.await() }, "$row: the drain completes")
                val s = rig.inner.observed()
                assertEquals(1, s.drainConsumedLevels, "$row: the drain consumed the level itself")
                assertTrue(done.await(), "$row: the object whose activity the drain consumed was never delivered without another nudge")
                assertData(row, outcome.get()!!.getOrThrow(), rig.track)
                val order = rig.inner.observed().callOrder
                val consumed = order.indexOf("drain-consumed-level")
                val delivered = order.lastIndexOf("receiver-poll-object")
                assertTrue(consumed in 0 until delivered, "$row: the delivering poll follows the drain's consumption of the level: $order")
                assertEquals(2, rig.inner.observed().objectPolls.size, "$row: the initial Empty and the one poll that delivered")
            } finally {
                if (drainHeld) rig.inner.releaseDrain()
            }
        }

    // -- cancellation controls -------------------------------------------------------

    @Test
    fun `cancellation between attempts prevents another attempt, judged after a later activity was consumed and a new empty parked wait followed`() =
        runTest {
            val rig = txRig()
            rig.inner.senderWriteDefault = BackendWriteResult.WouldBlock
            val manual = ManualDispatcher()
            val scope = manualScope(manual)
            val slot = CompletableDeferred<Result<WriteOutcome>>()
            // The first native write is HELD inside the call, so the attempt
            // cannot finish -- and its result cannot return synchronously --
            // while the caller is still running its start.
            rig.inner.holdNextSenderWrite()
            var held = true
            try {
                val writer = scope.launch { slot.complete(runCatching { rig.sender.write(rig.track, OBJ, Duration.INFINITE) }) }
                assertTrue(manual.runOne(), "R5 cancel: the writer starts and submits its first attempt")
                assertTrue(rig.inner.awaitCondition("the first attempt is in flight and held") { it.senderWriteHeld }, "R5 cancel: the first attempt never reached the held write")
                assertTrue(writer.isActive && !slot.isCompleted && manual.queued() == 0, "R5 cancel: the writer is provably suspended with nothing dispatched")
                assertEquals(0, rig.inner.observed().senderWrites.size, "R5 cancel: the held attempt has not answered yet")
                rig.inner.releaseSenderWrite()
                held = false
                assertTrue(manual.awaitDispatched(2), "R5 cancel: the attempt's committed result is dispatched to the writer")
                assertEquals(1, manual.queued(), "R5 cancel: exactly the first-result resumption is queued, not yet run")
                assertEquals(listOf(BackendWriteResult.WouldBlock), rig.inner.observed().senderWrites.map { it.result }, "R5 cancel: one refused attempt")
                // The result is dispatched but the writer has not resumed: cancel here, then release it.
                writer.cancel()
                manual.runAll()
                val r = withTimeout(BOUND) { slot.await() }
                assertTrue(r.exceptionOrNull() is kotlinx.coroutines.CancellationException, "R5 cancel: the writer resolves as cancellation between attempts; got $r")
                assertTrue(writer.isCompleted && manual.queued() == 0, "R5 cancel: the cancelled writer has COMPLETED and nothing is queued")
                // The later-activity oracle: an empty parked wait first, one
                // coherent baseline, genuine activity, then its consumption
                // followed by a NEW empty parked wait entry. Only then judge.
                assertTrue(rig.inner.awaitParked("an empty parked wait before the later activity"), "R5 cancel: no empty parked wait after the cancellation settled")
                val base = rig.inner.observed()
                val baseActivity = base.waitReturns.count { it == BackendWaitResult.ACTIVITY }
                rig.inner.senderWriteDefault = BackendWriteResult.Accepted
                rig.inner.markActivity()
                assertTrue(
                    rig.inner.awaitCondition("the later activity is consumed and a new empty parked wait follows") {
                        it.waitReturns.count { w -> w == BackendWaitResult.ACTIVITY } == baseActivity + 1 &&
                            it.waitEntries == base.waitEntries + 1 && it.waitParked && !it.activityLevel
                    },
                    "R5 cancel: the later activity was never consumed, or no new empty parked wait followed its pass",
                )
                assertEquals(1, rig.inner.observed().senderWrites.size, "R5 cancel: a cancelled writer must not attempt again")
                assertEquals(0, rig.inner.observed().acceptedPayloads.size, "R5 cancel: nothing transferred")
                assertAttemptsCarry("R5 cancel", rig)
            } finally {
                if (held) rig.inner.releaseSenderWrite()
            }
        }

    @Test
    fun `an in-flight committed write result stays committed although the caller was cancelled`() =
        runTest {
            val rig = txRig()
            var held = true
            try {
                rig.inner.holdNextSenderWrite()
                val slot = CompletableDeferred<Result<WriteOutcome>>()
                val writer = launch(Dispatchers.Default) { slot.complete(runCatching { rig.sender.write(rig.track, OBJ, Duration.INFINITE) }) }
                assertTrue(rig.inner.awaitCondition("the write is in flight and held") { it.senderWriteHeld }, "R5 committed: the write was never held")
                writer.cancel()
                rig.inner.releaseSenderWrite()
                held = false
                val r = withTimeout(BOUND) { slot.await() }
                assertEquals(WriteOutcome.Accepted, r.getOrThrow(), "R5 committed: the in-flight attempt's outcome is delivered, not a cancellation")
                assertEquals(listOf("k"), rig.inner.observed().acceptedPayloads.map { String(it) }, "R5 committed: the transfer happened exactly once")
                assertAttemptsCarry("R5 committed", rig)
                withTimeout(BOUND) { writer.join() }
            } finally {
                if (held) rig.inner.releaseSenderWrite()
            }
        }
}
