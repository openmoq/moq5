package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BackendTrackEventResult
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Receiver DISCOVERY and the durable track inventory, RED-first over the
 * scripted backend (`media_receiver.h` §6.2, `media_receiver.c`
 * poll_track; the Swift receiver as precedent).
 *
 * Derived contract:
 *  * Track events are drained from the native receiver ON THE SERVICE WORKER
 *    on every pass, right after the endpoint's wait returns and BEFORE the
 *    pass takes the endpoint's record -- so the inventory a collector sees
 *    at a record is already integrated, and (later) no object for a track
 *    can be surfaced before its ADDED. The drain runs to empty: with no
 *    exact history staged, nothing the binding does can fill the C queue
 *    (whose overflow is TERMINAL for the receiver).
 *  * `inventory: StateFlow<ReceiverInventory>` is ONE atomic durable
 *    snapshot: the tracks in discovery order (retaining removed and ended
 *    ones with their presence, as the C handles are retained), catalog
 *    readiness, the receiver's terminal, if any, and a monotonic `revision`
 *    that advances on EVERY integrated change -- so a collector wakes for an
 *    in-place update or an end that leaves the track count unchanged, which
 *    a `StateFlow` comparing equal lists would otherwise swallow.
 *  * `MediaTrack` is identity: the same handle is always the same instance;
 *    its description is an immutable snapshot re-taken only on UPDATED (the
 *    one sanctioned mutation, live -> VOD -- an EVENT, not a presence; the
 *    track stays Present). After the receiver closes the inventory freezes
 *    and every track reads `isRetired`.
 *  * A receiver terminal carries the receiver's OWN fatal code domain
 *    (`moq_media_receiver_fatal_code`), never a MoQ wire error code.
 *  * Exact history (1057 F1): `EventHistoryPolicy.Disabled` by default -- no
 *    history is staged and a consumer-less app never fails; `Exact(capacity)`
 *    stages a bounded FIFO behind `trackEvents` (single consumer). Overflow is
 *    STREAM-LOCAL: the prefix delivers, then `next()` fails with the witness,
 *    while the receiver and its inventory stay healthy.
 *  * The history is exact for the public discovery subset (ADDED, UPDATED,
 *    REMOVED, ENDED, CATALOG_READY). Coalesced parse-drop and subscription
 *    acknowledgements are drained and counted on the track; they neither
 *    enter the stream nor consume its capacity.
 *  * A bridge answer on the poll quarantines; a C terminal (CLOSED, fatal or
 *    clean) is a durable value on the inventory and the end of the stream.
 */
internal class PublicReceiverInventoryTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false)
        val T1 = -0x7fff_ffff_ffff_fefdL // scripted track tokens: bit 63 set, kind 3
        val T2 = -0x7fff_ffff_ffff_fdfdL
        val VIDEO = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()), isLive = true)
        val AUDIO = TrackDescription(name = MediaBytes("b".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("audio".toByteArray()), isLive = true)
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        fun updated(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Updated, t, d)
        fun removed(t: Long) = BackendTrackEvent(BackendTrackEventKind.Removed, t, null)
        fun ended(t: Long) = BackendTrackEvent(BackendTrackEventKind.Ended, t, null)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    @Test
    fun `the inventory starts empty and not ready, and integrates discovery in order on the worker`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            assertEquals(ReceiverInventory(emptyList(), catalogReady = false, terminal = null, anomalies = 0, revision = 0uL), receiver.inventory.value)
            backend.scriptTrackEvents(added(T1, VIDEO), added(T2, AUDIO), ready)
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            assertEquals(listOf(VIDEO, AUDIO), inv.tracks.map { it.description })
            assertEquals(listOf(TrackPresence.Present, TrackPresence.Present), inv.tracks.map { it.presence })
            assertEquals(3uL, inv.revision, "one revision per integrated event")
            assertTrue(backend.observed().trackPolls.all { it.threadId == backend.observed().serviceThreadId }, "drained on the worker")
            endpoint.closeAndJoin()
        }

    @Test
    fun `events are drained to empty before the pass takes the endpoint record, so a collector at the record already sees the tracks`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            val seenAtRecord = async(Dispatchers.Default) {
                // The record the pass publishes: what the inventory holds at that instant.
                endpoint.snapshots.first { it.state == EndpointState.Established }
                receiver.inventory.value.tracks.size
            }
            assertTrue(backend.awaitCondition("the collector is parked on the records") { it.waitParked })
            val before = backend.observed().callOrder.size
            backend.snapshotResult = BackendSnapshotResult.Ok(EndpointSnapshot(state = EndpointState.Established, negotiatedVersion = null, terminal = null))
            backend.scriptTrackEvents(added(T1, VIDEO), added(T2, AUDIO))
            backend.markActivity()
            assertEquals(2, withTimeout(BOUND) { seenAtRecord.await() }, "the inventory is integrated before the record is published")
            val pass = backend.observed().callOrder.drop(before)
            val polls = pass.indices.filter { pass[it] == "receiver-poll-track" }
            val record = pass.indexOf("snapshot")
            assertTrue(polls.size >= 3 && polls.all { it < record }, "every poll (two events and the empty answer) precedes the record; pass was $pass")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a track is identity, UPDATED re-snapshots its description in place and leaves it Present, and removed or ended tracks stay listed`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            backend.scriptTrackEvents(added(T1, VIDEO), added(T2, AUDIO), ready)
            backend.markActivity()
            val first = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            val video = first.tracks[0]
            val vod = VIDEO.copy(isLive = false, trackDurationMs = 90_000uL)
            backend.scriptTrackEvents(updated(T1, vod), ended(T2))
            backend.markActivity()
            val second = withTimeout(BOUND) { receiver.inventory.first { it.tracks[1].presence == TrackPresence.Ended } }
            assertTrue(second.tracks[0] === video, "the same handle is the same instance")
            assertEquals(vod, video.description, "the one sanctioned mutation re-snapshots the description in place")
            assertEquals(TrackPresence.Present, video.presence, "UPDATED is an event, not a presence")
            backend.scriptTrackEvents(removed(T1))
            backend.markActivity()
            val third = withTimeout(BOUND) { receiver.inventory.first { it.tracks[0].presence == TrackPresence.Removed } }
            assertEquals(2, third.tracks.size, "removed and ended tracks are retained, in discovery order")
            assertTrue(third.tracks[0] === video)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an update-only event wakes a collector on the inventory itself, with the same track count and instances`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            backend.scriptTrackEvents(added(T1, VIDEO), ready)
            backend.markActivity()
            val base = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            val waiter = async(Dispatchers.Default) { receiver.inventory.first { it.revision > base.revision } }
            assertTrue(backend.awaitCondition("the collector is parked on the inventory") { it.waitParked })
            val vod = VIDEO.copy(isLive = false)
            backend.scriptTrackEvents(updated(T1, vod))
            backend.markActivity()
            val woke = withTimeout(BOUND) { waiter.await() }
            assertEquals(base.revision + 1uL, woke.revision, "exactly the update advanced the revision; nothing else happened")
            assertEquals(1, woke.tracks.size)
            assertTrue(woke.tracks[0] === base.tracks[0])
            assertEquals(vod, woke.tracks[0].description)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an end-only event wakes a collector although the track count is unchanged`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            backend.scriptTrackEvents(added(T1, VIDEO), ready)
            backend.markActivity()
            val base = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            val waiter = async(Dispatchers.Default) { receiver.inventory.first { it.revision > base.revision } }
            assertTrue(backend.awaitCondition("the collector is parked on the inventory") { it.waitParked })
            backend.scriptTrackEvents(ended(T1))
            backend.markActivity()
            val woke = withTimeout(BOUND) { waiter.await() }
            assertEquals(base.revision + 1uL, woke.revision)
            assertEquals(1, woke.tracks.size)
            assertEquals(TrackPresence.Ended, woke.tracks[0].presence)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a dropped event and a terminal each advance the revision, so anomalies and terminals are observable too`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            backend.scriptTrackEvents(added(T1, VIDEO), ready)
            backend.markActivity()
            val base = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            backend.scriptTrackEvents(updated(T2, AUDIO)) // unknown handle: dropped and counted
            backend.markActivity()
            val counted = withTimeout(BOUND) { receiver.inventory.first { it.revision > base.revision } }
            assertEquals(1, counted.anomalies)
            assertEquals(base.tracks, counted.tracks)
            backend.pollTrackTerminal = BackendTrackEventResult.Closed(fatal = false, code = 0uL)
            backend.markActivity()
            val closed = withTimeout(BOUND) { receiver.inventory.first { it.revision > counted.revision } }
            assertEquals(ReceiverTerminal.Closed, closed.terminal)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an ADDED for a known handle, or an UPDATED, REMOVED or ENDED for an unknown one, is dropped without inventing or duplicating a track`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            backend.scriptTrackEvents(added(T1, VIDEO), added(T1, AUDIO), updated(T2, AUDIO), removed(T2), ended(T2), ready)
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            assertEquals(1, inv.tracks.size, "no duplicate for a known handle, no track for an unknown one")
            assertEquals(VIDEO, inv.tracks[0].description, "a duplicate ADDED does not overwrite the snapshot")
            assertEquals(4, receiver.inventory.value.anomalies, "each dropped event is counted, never silent")
            endpoint.closeAndJoin()
        }

    @Test
    fun `receiver close freezes the inventory and every track reads retired while its snapshot stays readable`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            backend.scriptTrackEvents(added(T1, VIDEO), ready)
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            val video = inv.tracks[0]
            assertTrue(!video.isRetired)
            receiver.close()
            assertEquals(ReceiverTerminal.Closed, receiver.inventory.value.terminal)
            assertTrue(video.isRetired, "tracks retire with their receiver")
            assertEquals(VIDEO, video.description, "the snapshot stays readable; it was copied, never borrowed")
            assertEquals(TrackPresence.Present, video.presence, "retirement is the receiver's, not a presence change")
            // Nothing is polled after close.
            val polls = backend.observed().trackPolls.size
            backend.markActivity()
            endpoint.closeAndJoin()
            assertEquals(polls, backend.observed().trackPolls.size)
        }

    @Test
    fun `with history disabled nothing is staged, events still integrate, and trackEvents is absent`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            assertEquals(EventHistoryPolicy.Disabled, CONFIG.eventHistory)
            assertEquals(null, receiver.trackEvents, "no exact stream unless armed at construction")
            backend.scriptTrackEvents(*Array(200) { added(T1 + it, VIDEO) })
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.tracks.size == 200 } }
            assertEquals(null, inv.terminal, "a consumer-less app never fails: nothing was staged")
            assertEquals(0, backend.observed().trackPollsLeftQueued, "the native queue is drained to empty on every pass")
            endpoint.closeAndJoin()
        }

    @Test
    fun `exact history delivers the prefix in order to one consumer and fails stream-locally on overflow while the inventory stays healthy`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG.copy(eventHistory = EventHistoryPolicy.Exact(capacity = 2)))
            val stream = receiver.trackEvents
            assertTrue(stream != null)
            backend.scriptTrackEvents(added(T1, VIDEO), added(T2, AUDIO), ready)
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            assertEquals(2, inv.tracks.size, "the inventory integrated everything")
            assertEquals(null, inv.terminal, "an overflowed history is not a receiver terminal")
            val e1 = withTimeout(BOUND) { stream!!.next() }
            val e2 = withTimeout(BOUND) { stream!!.next() }
            assertTrue(e1 is TrackEvent.Added && e1.track === inv.tracks[0])
            assertTrue(e2 is TrackEvent.Added && e2.track === inv.tracks[1])
            val overflow = assertFailsWith<TrackEventHistoryOverflowException> { withTimeout(BOUND) { stream!!.next() } }
            assertEquals(1uL, overflow.dropped, "the witness: how many events the history could not keep")
            assertEquals(2, overflow.capacity)
            endpoint.closeAndJoin()
        }

    @Test
    fun `exact history admits one consumer at a time`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG.copy(eventHistory = EventHistoryPolicy.Exact(capacity = 8)))
            val stream = receiver.trackEvents!!
            // Enter next() before launching the competing consumer. The
            // backend can already be parked before either consumer starts.
            val first = async(Dispatchers.Default, start = CoroutineStart.UNDISPATCHED) { stream.next() }
            assertTrue(first.isActive && !first.isCompleted, "the first consumer is suspended in next()")
            assertFailsWith<IllegalStateException> { stream.next() }
            backend.scriptTrackEvents(ready)
            backend.markActivity()
            assertEquals(TrackEvent.CatalogReady, withTimeout(BOUND) { first.await() })
            endpoint.closeAndJoin()
        }

    @Test
    fun `parse-drop and subscription acknowledgements are drained and counted on the track, and neither enter the history nor consume its capacity`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG.copy(eventHistory = EventHistoryPolicy.Exact(capacity = 2)))
            backend.scriptTrackEvents(
                added(T1, VIDEO),
                BackendTrackEvent(BackendTrackEventKind.ParseDrop, T1, null, parseDropsTotal = 3uL, parseDropsDelta = 3uL),
                BackendTrackEvent(BackendTrackEventKind.UpdateOk, T1, null),
                BackendTrackEvent(BackendTrackEventKind.ParseDrop, T1, null, parseDropsTotal = 5uL, parseDropsDelta = 2uL),
                ready,
            )
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            assertEquals(5uL, inv.tracks[0].parseDrops, "durable cumulative diagnostics on the track")
            val stream = receiver.trackEvents!!
            assertTrue(withTimeout(BOUND) { stream.next() } is TrackEvent.Added)
            assertEquals(TrackEvent.CatalogReady, withTimeout(BOUND) { stream.next() }, "two public records fit a capacity of two: the three filtered records consumed nothing")
            assertEquals(0, backend.observed().trackPollsLeftQueued)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a C terminal on the poll is a durable value and ends the stream, while a bridge answer quarantines`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG.copy(eventHistory = EventHistoryPolicy.Exact(capacity = 8)))
            backend.scriptTrackEvents(added(T1, VIDEO))
            backend.pollTrackTerminal = BackendTrackEventResult.Closed(fatal = true, code = 0x2uL)
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.terminal != null } }
            assertEquals(ReceiverTerminal.Fatal(ReceiverFatalCode(0x2uL)), inv.terminal, "the receiver's own code domain, full width")
            val stream = receiver.trackEvents!!
            assertTrue(withTimeout(BOUND) { stream.next() } is TrackEvent.Added, "the prefix still delivers")
            val fatal = assertFailsWith<ReceiverFatalException> { withTimeout(BOUND) { stream.next() } }
            assertEquals(ReceiverFatalCode(0x2uL), fatal.code)

            val backend2 = newBackend()
            val endpoint2 = MoqEndpoint.open(EndpointConfig(URL)) { backend2 }
            val receiver2 = endpoint2.attachReceiver(CONFIG)
            backend2.pollTrackTerminal = BackendTrackEventResult.Bridge(BridgeFailure("receiver-poll-track", -1002))
            backend2.markActivity()
            val quarantined = runCatching { withTimeout(BOUND) { endpoint2.awaitEstablished(BOUND) } }.exceptionOrNull()
            assertTrue(quarantined is BridgeContractException && quarantined.failure.code == -1002, "a bridge answer is a quarantine, not a receiver terminal; saw $quarantined")
            assertEquals(null, receiver2.inventory.value.terminal)
            endpoint.closeAndJoin()
            backend2.expireWait()
            runCatching { endpoint2.closeAndJoin() }
        }
}
