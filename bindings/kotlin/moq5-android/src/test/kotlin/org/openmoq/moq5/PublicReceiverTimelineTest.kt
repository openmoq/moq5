package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendSapRecord
import org.openmoq.moq5.engine.BackendTimelineRecord
import org.openmoq.moq5.engine.BackendRecordResult
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The receiver's SAP-record and media-timeline-record streams, over the
 * scripted backend (`media_receiver.h`: `moq_media_receiver_poll_sap`
 * / `_poll_media_timeline`).
 *
 * Derived contract, parallel to [MediaReceiver.objects]:
 *  * PULL ONLY: nothing polls records on a service pass; one `next()` is one
 *    native poll on the worker, preceded by the ordinary track drain, and
 *    followed by another drain when the record's track is not yet in the
 *    inventory -- a delivered record carries the inventory's own
 *    [MediaTrack] instance.
 *  * The stream's elements are the records themselves; an empty answer
 *    parks the consumer (`next()` and `asFlow()` alike) and is never a
 *    terminal.
 *  * Value domains: `ept_ms` and `media_time_ms` are [MediaMillis];
 *    `wallclock_ms` is [UnixMillis] with native zero as `null`; group and
 *    object ids keep all 64 bits; [SapType] is the closed NONE/1/2/3 set
 *    and an unknown native value fails closed by field name.
 *  * One consumer per stream; a clean terminal is `null` after the prefix;
 *    a fatal terminal throws [ReceiverFatalException]. The native polls do
 *    not consult the endpoint latch (only `poll_object` does), so interrupt
 *    is not an outcome here.
 */
internal class PublicReceiverTimelineTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val MANUAL = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val T2 = -0x7fff_ffff_ffff_fdfdL
        val SAP = TrackDescription(name = MediaBytes("sap".toByteArray()), packaging = MediaBytes("eventtimeline".toByteArray()), eventType = MediaBytes("org.ietf.moq.cmsf.sap".toByteArray()))
        val HISTORY = TrackDescription(name = MediaBytes("history".toByteArray()), packaging = MediaBytes("mediatimeline".toByteArray()))
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)
        fun sap(t: Long, objectId: ULong, type: SapType = SapType.Type2, ept: ULong = objectId * 10uL) = BackendSapRecord(track = t, location = ObjectLocation(7uL, objectId), sapType = type, earliestPresentationTime = MediaMillis(ept))
        fun tl(t: Long, objectId: ULong, wallclock: ULong = 1_700_000_000_000uL + objectId) = BackendTimelineRecord(track = t, mediaTime = MediaMillis(objectId * 40uL), location = ObjectLocation(7uL, objectId), wallclock = UnixMillis.fromNativeOrNull(wallclock))
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    /* Each helper fails by name when the stream ended instead of delivering. */
    private fun sapOf(e: SapRecord?): SapRecord = checkNotNull(e) { "expected a sap record, got the end of the stream" }

    private fun timelineOf(e: MediaTimelineRecord?): MediaTimelineRecord = checkNotNull(e) { "expected a media-timeline record, got the end of the stream" }

    private suspend fun ExactStream<SapRecord>.nextSapRecord(): SapRecord = sapOf(withTimeout(BOUND) { next() })

    private suspend fun ExactStream<MediaTimelineRecord>.nextTimelineRecord(): MediaTimelineRecord = timelineOf(withTimeout(BOUND) { next() })

    private suspend fun discovered(backend: ScriptedEndpointBackend, receiver: MediaReceiver): ReceiverInventory {
        backend.scriptTrackEvents(added(T1, SAP), added(T2, HISTORY), ready)
        backend.markActivity()
        return withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
    }

    @Test
    fun `records are pulled on demand only, one native poll per next, on the worker, and carry the inventory's track instance`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            val (sapTrack, historyTrack) = discovered(backend, receiver).tracks
            backend.scriptSapRecords(sap(T1, 0uL), sap(T1, 1uL, SapType.Type3, 2100uL))
            backend.scriptTimelineRecords(tl(T2, 0uL, wallclock = 0uL), tl(T2, 1uL))
            backend.markActivity()
            backend.markActivity()
            assertTrue(backend.awaitCondition("passes ran") { it.waitEntries >= 3 })
            assertEquals(0, backend.observed().sapPolls.size + backend.observed().timelinePolls.size, "no demand, no poll")
            val s0 = receiver.sapRecords.nextSapRecord()
            assertTrue(s0.track === sapTrack)
            assertEquals(ObjectLocation(groupId = 7uL, objectId = 0uL), s0.location)
            assertEquals(SapType.Type2, s0.sapType)
            assertEquals(MediaMillis(0uL), s0.earliestPresentationTime)
            assertEquals(1, backend.observed().sapPolls.size)
            assertEquals(backend.observed().serviceThreadId, backend.observed().sapPolls[0].threadId)
            val s1 = receiver.sapRecords.nextSapRecord()
            assertEquals(SapType.Type3, s1.sapType)
            assertEquals(MediaMillis(2100uL), s1.earliestPresentationTime, "ept_ms is milliseconds, never widened to micros")
            val h0 = receiver.mediaTimeline.nextTimelineRecord()
            assertTrue(h0.track === historyTrack)
            assertEquals(MediaMillis(0uL), h0.mediaTime)
            assertEquals(null, h0.wallclock, "native wallclock 0 is absent (VOD/unknown), never 1970")
            val h1 = receiver.mediaTimeline.nextTimelineRecord()
            assertEquals(UnixMillis(1_700_000_000_001uL), h1.wallclock)
            assertEquals(MediaMillis(40uL), h1.mediaTime)
            assertEquals(ObjectLocation(7uL, 1uL), h1.location)
            assertEquals(1, backend.observed().timelinePolls.size - 1, "one demand is one poll (two demands, two polls)")
            endpoint.closeAndJoin()
        }

    @Test
    fun `group and object ids keep all 64 bits`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            backend.scriptSapRecords(BackendSapRecord(T1, ObjectLocation(ULong.MAX_VALUE, 0x8000_0000_0000_0001uL), SapType.None, MediaMillis(ULong.MAX_VALUE)))
            val s = receiver.sapRecords.nextSapRecord()
            assertEquals(ULong.MAX_VALUE, s.location.groupId)
            assertEquals(0x8000_0000_0000_0001uL, s.location.objectId)
            assertEquals(MediaMillis(ULong.MAX_VALUE), s.earliestPresentationTime)
            assertEquals(SapType.None, s.sapType)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a record for a track whose ADDED is queued but not drained makes the demand drain again, and the record names the discovered track`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            val T3 = -0x7fff_ffff_ffff_fcfdL
            backend.holdNextSapPoll()
            val first = async(Dispatchers.Default) { receiver.sapRecords.next() }
            try {
                assertTrue(backend.awaitCondition("the poll is held") { it.sapPollHeld })
                backend.scriptTrackEvents(added(T3, SAP.copy(name = MediaBytes("sap2".toByteArray()))))
                backend.scriptSapRecords(sap(T3, 0uL))
            } finally {
                backend.releaseSapPoll()
            }
            val s = sapOf(withTimeout(BOUND) { first.await() })
            assertTrue(s.track === receiver.inventory.value.tracks[2], "the record's track is the inventory's instance, discovered by the post-poll drain")
            assertEquals(MediaBytes("sap2".toByteArray()), s.track.description.name)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a record for a track no ADDED ever announces is a bridge-contract failure after the second drain`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            backend.scriptTimelineRecords(tl(-0x7fff_ffff_ffff_fbfdL, 0uL))
            val failure = assertFailsWith<BridgeContractException> { withTimeout(BOUND) { receiver.mediaTimeline.next() } }
            assertEquals("receiver-timeline-track", failure.failure.op)
            assertEquals(2, receiver.inventory.value.tracks.size, "no track invented")
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }

    @Test
    fun `each stream admits one consumer at a time, and the two streams are independent consumers`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            val parked = async(Dispatchers.Default) { receiver.sapRecords.next() }
            assertTrue(backend.awaitCondition("one empty sap poll, parked") { it.sapPolls.size == 1 })
            assertFailsWith<IllegalStateException> { receiver.sapRecords.next() }
            backend.scriptTimelineRecords(tl(T2, 0uL))
            assertEquals(MediaMillis(0uL), receiver.mediaTimeline.nextTimelineRecord().mediaTime, "the other stream is not blocked by a parked sap consumer")
            backend.scriptSapRecords(sap(T1, 0uL))
            backend.markActivity()
            assertEquals(0uL, sapOf(withTimeout(BOUND) { parked.await() }).location.objectId)
            endpoint.closeAndJoin()
        }

    @Test
    fun `asFlow keeps collecting past an empty answer -- an empty queue never ends a collection`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            backend.scriptTimelineRecords(tl(T2, 0uL), tl(T2, 1uL))
            val collector = async(Dispatchers.Default) { receiver.mediaTimeline.asFlow().take(3).toList() }
            assertTrue(backend.awaitCondition("the collection is parked on an empty answer") { it.timelinePolls.size >= 3 })
            backend.scriptTimelineRecords(tl(T2, 2uL))
            backend.markActivity()
            val got = withTimeout(BOUND) { collector.await() }
            assertEquals(listOf(0uL, 1uL, 2uL), got.map { it.location.objectId }, "two records, a parked empty answer, then the third record")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a clean terminal ends each stream with null after its prefix, and a fatal one throws with the receiver's code`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            backend.scriptSapRecords(sap(T1, 0uL))
            backend.pollSapTerminal = BackendRecordResult.Closed(fatal = false, code = 0uL)
            assertEquals(0uL, receiver.sapRecords.nextSapRecord().location.objectId)
            assertEquals(null, withTimeout(BOUND) { receiver.sapRecords.next() })
            assertEquals(null, withTimeout(BOUND) { receiver.sapRecords.next() }, "and it stays ended")
            val backend2 = newBackend()
            val endpoint2 = MoqEndpoint.open(EndpointConfig(URL)) { backend2 }
            val receiver2 = endpoint2.attachReceiver(MANUAL)
            discovered(backend2, receiver2)
            backend2.scriptTimelineRecords(tl(T2, 0uL))
            backend2.pollTimelineTerminal = BackendRecordResult.Closed(fatal = true, code = 0x2uL)
            assertEquals(0uL, receiver2.mediaTimeline.nextTimelineRecord().location.objectId)
            assertEquals(ReceiverFatalCode.EVENT_OVERFLOW, assertFailsWith<ReceiverFatalException> { withTimeout(BOUND) { receiver2.mediaTimeline.next() } }.code)
            assertEquals(ReceiverTerminal.Fatal(ReceiverFatalCode.EVENT_OVERFLOW), receiver2.inventory.value.terminal)
            endpoint.closeAndJoin()
            endpoint2.closeAndJoin()
        }

    @Test
    fun `receiver close ends both streams and nothing polls after it`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            backend.scriptSapRecords(sap(T1, 0uL))
            receiver.close()
            assertEquals(null, withTimeout(BOUND) { receiver.sapRecords.next() })
            assertEquals(null, withTimeout(BOUND) { receiver.mediaTimeline.next() })
            assertEquals(0, backend.observed().sapPolls.size + backend.observed().timelinePolls.size)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a bridge answer on a record poll quarantines and is not a receiver terminal`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(MANUAL)
            discovered(backend, receiver)
            backend.pollSapTerminal = BackendRecordResult.Bridge(org.openmoq.moq5.engine.BridgeFailure("receiver-poll-sap", -1002))
            assertEquals(-1002, assertFailsWith<BridgeContractException> { withTimeout(BOUND) { receiver.sapRecords.next() } }.failure.code)
            assertEquals(null, receiver.inventory.value.terminal)
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }
}
