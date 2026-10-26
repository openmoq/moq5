package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendStatsResult
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendTrackEventResult
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The receiver's statistics snapshot, RED-first over the scripted backend
 * (`media_receiver.h`: `moq_media_receiver_stats_t` and
 * `moq_media_receiver_get_stats`).
 *
 * Derived contract:
 *  * [MediaReceiver.statsSnapshot] is ONE native read on the service
 *    worker, on demand, of the whole struct into one carrier -- a snapshot
 *    the C takes under its own mutex, published as an immutable
 *    [ReceiverStats] value. There is no stats flow in v1: the C offers no
 *    change signal for these fields, and `objectsQueued` / `bytesQueued`
 *    move on every object pull and every intake, so a flow refreshed on the
 *    engine's pass cadence would be stale between passes while implying
 *    freshness. A snapshot says exactly when it was taken.
 *  * Counters keep all 64 bits as `ULong`; the two booleans are strict
 *    (a carrier value outside 0/1 fails closed by field name).
 *  * [ReceiverStats.catalogComplete] is MSF §5.1.3 catalog completion,
 *    latched by an `isComplete` catalog: not the receiver's terminal, not a
 *    track's end, not receiver close.
 *  * The drop and deferral counters are distinct fields, never conflated,
 *    and the deferral counters are the very counters the record streams
 *    witness.
 *  * `objectsQueued` is the NATIVE queue depth: an object the engine has
 *    already dequeued into a stream's handoff slot is no longer counted.
 *  * A closed (detached) receiver refuses with [ReceiverClosedException];
 *    a receiver at its terminal still answers (the C answers).
 */
internal class PublicReceiverStatsTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val VIDEO = TrackDescription(name = MediaBytes("v".toByteArray()), packaging = MediaBytes("loc".toByteArray()))
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)
        fun raw(t: Long, bytes: String) =
            BackendObject(track = t, payload = MediaPayload.Raw(MediaBytes(bytes.toByteArray())), status = ObjectStatus.Normal, endOfGroup = false, viaDatagram = false, keyframe = true, captureTime = null, decodeTime = MediaMicros(0uL), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(0uL))
        val ZERO = ReceiverStats(0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, 0uL, false, 0uL, false, 0uL, 0uL)
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun discovered(backend: ScriptedEndpointBackend, receiver: MediaReceiver): MediaTrack {
        backend.scriptTrackEvents(added(T1, VIDEO), ready)
        backend.markActivity()
        return withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }.tracks[0]
    }

    @Test
    fun `a snapshot is one native read on the worker, on demand, with every field named and zero at rest`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.markActivity()
            assertTrue(backend.awaitCondition("a pass ran") { it.waitEntries >= 2 })
            assertEquals(0, backend.observed().statsReads.size, "no demand, no native read: nothing refreshes stats on a pass")
            val s = withTimeout(BOUND) { receiver.statsSnapshot() }
            assertEquals(ZERO, s)
            assertEquals(1, backend.observed().statsReads.size)
            assertEquals(backend.observed().serviceThreadId, backend.observed().statsReads[0].threadId, "read on the service worker")
            assertEquals(ReceiverStats(objectsReceived = 0uL, objectsQueued = 0uL, bytesQueued = 0uL, objectsDropped = 0uL, groupsDropped = 0uL, keyframesDropped = 0uL, parseDrops = 0uL, overflowEvents = 0uL, pauseTransitions = 0uL, paused = false, catalogDrops = 0uL, catalogComplete = false, sapRecordDeferrals = 0uL, mediaTimelineRecordDeferrals = 0uL), s, "the public field names, exactly")
            endpoint.closeAndJoin()
        }

    @Test
    fun `every counter maps to its own field, full width -- distinct values prove nothing is conflated or swapped`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptedStats = ReceiverStats(
                objectsReceived = 0x8000_0000_0000_0001uL, objectsQueued = 2uL, bytesQueued = ULong.MAX_VALUE, objectsDropped = 4uL,
                groupsDropped = 5uL, keyframesDropped = 6uL, parseDrops = 7uL, overflowEvents = 8uL, pauseTransitions = 9uL, paused = true,
                catalogDrops = 11uL, catalogComplete = false, sapRecordDeferrals = 13uL, mediaTimelineRecordDeferrals = 14uL,
            )
            val s = withTimeout(BOUND) { receiver.statsSnapshot() }
            assertEquals(0x8000_0000_0000_0001uL, s.objectsReceived, "above Long.MAX_VALUE, reconstructed through toULong()")
            assertEquals(ULong.MAX_VALUE, s.bytesQueued)
            assertEquals(4uL, s.objectsDropped)
            assertEquals(5uL, s.groupsDropped)
            assertEquals(6uL, s.keyframesDropped)
            assertEquals(7uL, s.parseDrops)
            assertEquals(8uL, s.overflowEvents)
            assertEquals(9uL, s.pauseTransitions)
            assertTrue(s.paused)
            assertEquals(11uL, s.catalogDrops)
            assertTrue(!s.catalogComplete)
            assertEquals(13uL, s.sapRecordDeferrals)
            assertEquals(14uL, s.mediaTimelineRecordDeferrals, "the timeline counter is not the sap counter")
            endpoint.closeAndJoin()
        }

    @Test
    fun `catalog completion is MSF completion -- distinct from the receiver's terminal, a track's end, and receiver close`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            val video = discovered(backend, receiver)
            backend.scriptedStats = ZERO.copy(catalogComplete = true)
            val complete = withTimeout(BOUND) { receiver.statsSnapshot() }
            assertTrue(complete.catalogComplete)
            assertEquals(null, receiver.inventory.value.terminal, "completion is not a terminal")
            assertEquals(TrackPresence.Present, video.presence, "nor a track state")
            backend.scriptTrackEvents(BackendTrackEvent(BackendTrackEventKind.Ended, T1, null))
            backend.scriptedStats = ZERO
            backend.markActivity()
            withTimeout(BOUND) { receiver.inventory.first { it.tracks[0].presence == TrackPresence.Ended } }
            assertTrue(!withTimeout(BOUND) { receiver.statsSnapshot() }.catalogComplete, "a track's end does not complete the catalog")
            backend.pollTrackTerminal = BackendTrackEventResult.Closed(fatal = false, code = 0uL)
            backend.markActivity()
            withTimeout(BOUND) { receiver.inventory.first { it.terminal != null } }
            assertTrue(!withTimeout(BOUND) { receiver.statsSnapshot() }.catalogComplete, "a terminal receiver still answers, and is not completion")
            receiver.close()
            assertFailsWith<ReceiverClosedException> { receiver.statsSnapshot() }
            endpoint.closeAndJoin()
        }

    @Test
    fun `objectsQueued is the native depth -- an object staged in the stream's handoff slot is no longer counted`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "a"), raw(T1, "b"), raw(T1, "c"))
            assertEquals(3uL, withTimeout(BOUND) { receiver.statsSnapshot() }.objectsQueued, "three queued natively, none pulled")
            // A demand cancelled after its poll began leaves the object in the handoff slot: dequeued natively.
            backend.holdNextObjectPoll()
            val cancelled = async(Dispatchers.Default) { receiver.objects.next() }
            try {
                assertTrue(backend.awaitCondition("held") { it.objectPollHeld })
                cancelled.cancel()
            } finally {
                backend.releaseObjectPoll()
            }
            runCatching { withTimeout(BOUND) { cancelled.await() } }
            assertTrue(backend.awaitCondition("the poll completed") { it.objectPolls.size == 1 })
            assertEquals(2uL, withTimeout(BOUND) { receiver.statsSnapshot() }.objectsQueued, "the staged object left the native queue; the JVM slot is not native depth")
            assertEquals("a", String(withTimeout(BOUND) { receiver.objects.next() }!!.mediaBytes().bytes()), "and it is still delivered")
            assertEquals(2uL, withTimeout(BOUND) { receiver.statsSnapshot() }.objectsQueued)
            endpoint.closeAndJoin()
        }

    @Test
    fun `the deferral counters are the counters the record streams witness`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.sapDeferrals = 44uL
            backend.timelineDeferrals = 3uL
            val s = withTimeout(BOUND) { receiver.statsSnapshot() }
            assertEquals(44uL, s.sapRecordDeferrals)
            assertEquals(3uL, s.mediaTimelineRecordDeferrals)
            assertEquals(ReceiverRecordEvent.Deferral(44uL, 0uL), withTimeout(BOUND) { receiver.sapRecords.next() }, "the stream witnesses the same counter")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a bridge refusal on the read quarantines and is not a receiver outcome`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.statsResult = BackendStatsResult.Bridge(BridgeFailure("receiver-stats", -1002))
            assertEquals(-1002, assertFailsWith<BridgeContractException> { withTimeout(BOUND) { receiver.statsSnapshot() } }.failure.code)
            assertEquals(null, receiver.inventory.value.terminal)
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }
}
