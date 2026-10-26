package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendObjectResult
import org.openmoq.moq5.engine.BackendRecordResult
import org.openmoq.moq5.engine.BackendSapRecord
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BackendTimelineRecord
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendTrackEventResult
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Independent exhaustion of the receiver's native queues, RED-first over the
 * scripted backend.
 *
 * Source-derived contract (`service/src/media_receiver.c`): each data poll --
 * `poll_object`, `poll_sap`, `poll_media_timeline` -- answers `MOQ_ERR_CLOSED`
 * only when ITS OWN ring is empty and the receiver is terminal (its fatal
 * flag, or the endpoint closed). While that ring still holds accepted
 * entries it answers them, terminal or not, and `poll_track` has the same
 * shape for the track-event ring. A terminal is therefore ONE receiver-wide
 * reason, but exhaustion is per native queue: what a queue accepted before
 * the terminal is still owed to its consumer after another queue reported
 * the terminal first.
 *
 * The two terminal causes are scripted as the C presents them: the CLEAN
 * terminal is the ENDPOINT closing (the endpoint's wait answers CLOSED and
 * its record reads Closed/Unclassified; the receiver stays attached and is
 * never destroyed to manufacture it); the FATAL terminal is the receiver's
 * own fatal flag with the endpoint still established.
 *
 * What the rows pin:
 *  * a legal terminal receiver with distinct nonempty prefixes in the
 *    object, SAP and media-timeline queues delivers every prefix exactly
 *    once, in any consumption order, with the complete declared public
 *    shape of each element, then reports the terminal on each stream
 *    (clean: `null`; fatal: [ReceiverFatalException] with the receiver's own
 *    code); after every family has finished, the complete native poll
 *    inventory is final and repeated end calls poll nothing more;
 *  * the reverse interleaving: the track pre-drain answers Empty, a data
 *    poll is held while the final track events arrive and the endpoint
 *    closes, and the poll is released with CLOSED -- the queued track
 *    events are still integrated (per-track ENDED in the inventory, and in
 *    the exact history) before that stream reports the terminal;
 *  * per-track ENDED and a record Deferral are not receiver-wide terminals;
 *  * explicit receiver close stops native polling, and an already staged
 *    handoff keeps its delivery-before-terminal contract.
 */
internal class PublicReceiverQueueExhaustionTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false)
        val WITH_HISTORY = CONFIG.copy(eventHistory = EventHistoryPolicy.Exact(capacity = 8))
        val T1 = -0x7fff_ffff_ffff_fefdL
        val VIDEO = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        const val FATAL = 7uL
        val CLOSED_ENDPOINT = EndpointSnapshot(state = EndpointState.Closed, negotiatedVersion = null, terminal = EndpointTerminal.Unclassified)
        val ESTABLISHED_ENDPOINT = EndpointSnapshot(state = EndpointState.Established, negotiatedVersion = null, terminal = null)
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        fun ended(t: Long) = BackendTrackEvent(BackendTrackEventKind.Ended, t, null)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)

        /* The declared public shapes. Each declaration feeds BOTH the script
         * and the expectation; nothing is read back from an observation. The
         * values are what the RAW path of `media/object/src/media_object.c`
         * can produce: a Normal status with a payload (a non-Normal status
         * carries none), composition offset zero, and a present LOC capture
         * timestamp driving both decode and presentation time -- or all three
         * zero when the timestamp is absent. endOfGroup is the separate flag. */
        val CAPTURE = UnixMicros(1_700_000_000_000_005uL)
        val O1 =
            BackendObject(
                track = T1, payload = MediaPayload.Raw(MediaBytes("o1".toByteArray())), status = ObjectStatus.Normal, endOfGroup = true,
                viaDatagram = false, keyframe = true, captureTime = CAPTURE,
                decodeTime = MediaMicros(CAPTURE.value), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(CAPTURE.value),
            )
        val O2 =
            BackendObject(
                track = T1, payload = MediaPayload.Raw(MediaBytes("o2".toByteArray())), status = ObjectStatus.Normal, endOfGroup = false,
                viaDatagram = true, keyframe = false, captureTime = null,
                decodeTime = MediaMicros(0uL), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(0uL),
            )
        val S1 = BackendSapRecord(track = T1, location = ObjectLocation(7uL, 1uL), sapType = SapType.Type2, earliestPresentationTime = MediaMillis(10uL))
        val S2 = BackendSapRecord(track = T1, location = ObjectLocation(7uL, 2uL), sapType = SapType.Type3, earliestPresentationTime = MediaMillis(25uL))
        val M1 = BackendTimelineRecord(track = T1, mediaTime = MediaMillis(120uL), location = ObjectLocation(7uL, 3uL), wallclock = UnixMillis.fromNativeOrNull(1_700_000_000_003uL))
        val STAGED = O2.copy(payload = MediaPayload.Raw(MediaBytes("staged".toByteArray())))
        val UNDELIVERED = O2.copy(payload = MediaPayload.Raw(MediaBytes("undelivered".toByteArray())))
    }

    private enum class Family { Objects, Sap, Timeline }

    private class Rig(val backend: ScriptedEndpointBackend, val endpoint: MoqEndpoint, val receiver: MediaReceiver)

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun rig(config: ReceiverConfig = CONFIG): Rig {
        val backend = newBackend()
        val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { backend }, backend)
        return Rig(backend, endpoint, endpoint.attachReceiver(config))
    }

    // -- the complete declared shapes -----------------------------------------------

    private fun assertObject(row: String, what: String, o: MediaObject?, exp: BackendObject, track: MediaTrack) {
        assertTrue(o != null, "$row: $what was not delivered (got null)")
        assertTrue(o.track === track, "$row: $what names the inventory's own track instance")
        assertEquals(exp.payload, o.payload, "$row: $what payload")
        assertEquals(exp.status, o.status, "$row: $what status")
        assertEquals(exp.endOfGroup, o.endOfGroup, "$row: $what endOfGroup")
        assertEquals(exp.viaDatagram, o.viaDatagram, "$row: $what viaDatagram")
        assertEquals(exp.keyframe, o.keyframe, "$row: $what keyframe")
        assertEquals(exp.captureTime, o.captureTime, "$row: $what captureTime")
        assertEquals(exp.decodeTime, o.decodeTime, "$row: $what decodeTime")
        assertEquals(exp.compositionOffset, o.compositionOffset, "$row: $what compositionOffset")
        assertEquals(exp.presentationTime, o.presentationTime, "$row: $what presentationTime")
    }

    private fun assertSap(row: String, what: String, e: ReceiverRecordEvent<SapRecord>?, exp: BackendSapRecord, track: MediaTrack) {
        val rec = (e as? ReceiverRecordEvent.Record<*>)?.value as? SapRecord
        assertTrue(rec != null, "$row: $what was not delivered (got $e)")
        assertTrue(rec.track === track, "$row: $what names the inventory's own track instance")
        assertEquals(exp.location, rec.location, "$row: $what location")
        assertEquals(exp.sapType, rec.sapType, "$row: $what sapType")
        assertEquals(exp.earliestPresentationTime, rec.earliestPresentationTime, "$row: $what earliestPresentationTime")
    }

    private fun assertTimeline(row: String, what: String, e: ReceiverRecordEvent<MediaTimelineRecord>?, exp: BackendTimelineRecord, track: MediaTrack) {
        val rec = (e as? ReceiverRecordEvent.Record<*>)?.value as? MediaTimelineRecord
        assertTrue(rec != null, "$row: $what was not delivered (got $e)")
        assertTrue(rec.track === track, "$row: $what names the inventory's own track instance")
        assertEquals(exp.mediaTime, rec.mediaTime, "$row: $what mediaTime")
        assertEquals(exp.location, rec.location, "$row: $what location")
        assertEquals(exp.wallclock, rec.wallclock, "$row: $what wallclock")
    }

    /** The next element a stream still owes, or a named failure when the stream reported the terminal first. */
    private suspend fun <T> owed(row: String, what: String, next: suspend () -> T): T =
        try {
            withTimeout(BOUND) { next() }
        } catch (e: ReceiverFatalException) {
            throw AssertionError("$row: $what was owed by its native ring, but the receiver's terminal was reported first: $e", e)
        }

    /**
     * The legal terminal state, exactly as the C would present it: one track
     * discovered, a distinct nonempty prefix accepted in every data ring,
     * every native poll answering CLOSED once ITS ring is empty, and the ONE
     * cause: the endpoint closed (clean), or the receiver's fatal flag with
     * the endpoint still established.
     */
    private fun Rig.scriptTerminal(fatal: Boolean) {
        val code = if (fatal) FATAL else 0uL
        backend.scriptTrackEvents(added(T1, VIDEO), ready)
        backend.scriptObjects(O1, O2)
        backend.scriptSapRecords(S1, S2)
        backend.scriptTimelineRecords(M1)
        backend.pollTrackTerminal = BackendTrackEventResult.Closed(fatal, code)
        backend.pollObjectTerminal = BackendObjectResult.Closed(fatal, code)
        backend.pollSapTerminal = BackendRecordResult.Closed(fatal, code)
        backend.pollTimelineTerminal = BackendRecordResult.Closed(fatal, code)
        if (fatal) {
            backend.snapshotResult = BackendSnapshotResult.Ok(ESTABLISHED_ENDPOINT)
        } else {
            backend.snapshotResult = BackendSnapshotResult.Ok(CLOSED_ENDPOINT)
            backend.setClosed(true)
        }
    }

    private fun expectedTerminal(fatal: Boolean): ReceiverTerminal =
        if (fatal) ReceiverTerminal.Fatal(ReceiverFatalCode(FATAL)) else ReceiverTerminal.Closed

    private suspend fun Rig.endObjects(fatal: Boolean, row: String, what: String) {
        if (fatal) {
            val f = assertFailsWith<ReceiverFatalException>("$row: $what: the fatal terminal on the object stream") { withTimeout(BOUND) { receiver.objects.next() } }
            assertEquals(ReceiverFatalCode(FATAL), f.code, "$row: $what: the receiver's own fatal code")
        } else {
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "$row: $what: the clean end on the object stream")
        }
    }

    private suspend fun Rig.endSap(fatal: Boolean, row: String, what: String) {
        if (fatal) {
            val f = assertFailsWith<ReceiverFatalException>("$row: $what: the fatal terminal on the SAP stream") { withTimeout(BOUND) { receiver.sapRecords.next() } }
            assertEquals(ReceiverFatalCode(FATAL), f.code, "$row: $what: the receiver's own fatal code")
        } else {
            assertEquals(null, withTimeout(BOUND) { receiver.sapRecords.next() }, "$row: $what: the clean end on the SAP stream")
        }
    }

    private suspend fun Rig.endTimeline(fatal: Boolean, row: String, what: String) {
        if (fatal) {
            val f = assertFailsWith<ReceiverFatalException>("$row: $what: the fatal terminal on the timeline stream") { withTimeout(BOUND) { receiver.mediaTimeline.next() } }
            assertEquals(ReceiverFatalCode(FATAL), f.code, "$row: $what: the receiver's own fatal code")
        } else {
            assertEquals(null, withTimeout(BOUND) { receiver.mediaTimeline.next() }, "$row: $what: the clean end on the timeline stream")
        }
    }

    private suspend fun Rig.endHistory(fatal: Boolean, row: String, what: String) {
        val history = receiver.trackEvents!!
        if (fatal) {
            assertEquals(ReceiverFatalCode(FATAL), assertFailsWith<ReceiverFatalException>("$row: $what: the history ends with the fatal terminal") { withTimeout(BOUND) { history.next() } }.code)
        } else {
            assertEquals(null, withTimeout(BOUND) { history.next() }, "$row: $what: the history ends cleanly")
        }
    }

    private suspend fun Rig.consumeObjects(fatal: Boolean, row: String) {
        val track = receiver.inventory.value.tracks.single()
        val o1 = owed(row, "the first object") { receiver.objects.next() }
        assertTrue(o1 != null, "$row: the first object the native object ring still owes was not delivered (got null); a CLOSED from the track ring is not the object ring's exhaustion")
        assertObject(row, "object 1", o1, O1, track)
        assertObject(row, "object 2", owed(row, "the second object") { receiver.objects.next() }, O2, track)
        endObjects(fatal, row, "after the prefix")
        assertEquals(3, backend.observed().objectPolls.size, "$row: one native poll per owed object plus the one that answered CLOSED, never more")
        assertEquals(0, backend.observed().objectsLeftQueued, "$row: the object ring is exhausted")
    }

    private suspend fun Rig.consumeSap(fatal: Boolean, row: String) {
        val track = receiver.inventory.value.tracks.single()
        val e1 = owed(row, "SAP record 1") { receiver.sapRecords.next() }
        assertTrue(e1 != null, "$row: the SAP ring still owes record 1; got null")
        assertSap(row, "SAP record 1", e1, S1, track)
        assertSap(row, "SAP record 2", owed(row, "SAP record 2") { receiver.sapRecords.next() }, S2, track)
        endSap(fatal, row, "after the prefix")
        assertEquals(3, backend.observed().sapPolls.size, "$row: two owed SAP records plus the CLOSED poll")
    }

    private suspend fun Rig.consumeTimeline(fatal: Boolean, row: String) {
        val track = receiver.inventory.value.tracks.single()
        val e = owed(row, "the media-timeline record") { receiver.mediaTimeline.next() }
        assertTrue(e != null, "$row: the media-timeline ring still owes its record; got null")
        assertTimeline(row, "timeline record", e, M1, track)
        endTimeline(fatal, row, "after the prefix")
        assertEquals(2, backend.observed().timelinePolls.size, "$row: one owed timeline record plus the CLOSED poll")
    }

    private suspend fun Rig.consume(family: Family, fatal: Boolean, row: String) {
        when (family) {
            Family.Objects -> consumeObjects(fatal, row)
            Family.Sap -> consumeSap(fatal, row)
            Family.Timeline -> consumeTimeline(fatal, row)
        }
    }

    /** After EVERY family finished: the complete native inventory is final, and repeated end calls poll nothing more. */
    private suspend fun Rig.assertFinalInventory(fatal: Boolean, row: String) {
        val s = backend.observed()
        assertEquals(3, s.objectPolls.size, "$row: final object polls")
        assertEquals(3, s.sapPolls.size, "$row: final SAP polls")
        assertEquals(2, s.timelinePolls.size, "$row: final timeline polls")
        assertEquals(0, s.objectsLeftQueued, "$row: final object ring")
        assertEquals(0, s.trackPollsLeftQueued, "$row: final track ring")
        endObjects(fatal, row, "repeated end")
        endSap(fatal, row, "repeated end")
        endTimeline(fatal, row, "repeated end")
        endHistory(fatal, row, "repeated end")
        val after = backend.observed()
        assertEquals(s.objectPolls.size + s.sapPolls.size + s.timelinePolls.size, after.objectPolls.size + after.sapPolls.size + after.timelinePolls.size, "$row: repeated ends make no additional native poll")
        assertEquals(s.trackPolls.size, after.trackPolls.size, "$row: repeated ends drain nothing more")
    }

    private fun terminalRow(fatal: Boolean, order: List<Family>) =
        runTest {
            val row = "R2 ${if (fatal) "fatal" else "clean"}/${order.joinToString("-") { it.name.lowercase() }}"
            val rig = rig(WITH_HISTORY)
            rig.scriptTerminal(fatal)
            rig.backend.markActivity()
            val inv = withTimeout(BOUND) { rig.receiver.inventory.first { it.terminal != null } }
            assertEquals(expectedTerminal(fatal), inv.terminal, "$row: one receiver-wide terminal reason, recorded from the track ring")
            assertEquals(1, inv.tracks.size, "$row: the discovered track")
            assertTrue(inv.catalogReady, "$row: the catalog was ready before the terminal")
            assertEquals(0, rig.backend.observed().trackPollsLeftQueued, "$row: the track ring was exhausted before its CLOSED")
            // The cause is coherent with the endpoint's own record.
            val endpointState = withTimeout(BOUND) { rig.endpoint.snapshots.first { it.state != EndpointState.Connecting } }
            assertEquals(if (fatal) ESTABLISHED_ENDPOINT else CLOSED_ENDPOINT, endpointState, "$row: the endpoint's record matches the scripted cause")
            for (family in order) rig.consume(family, fatal, row)
            // The exact history: its own ring's prefix, then the same terminal.
            val history = rig.receiver.trackEvents!!
            val e1 = owed(row, "the discovery event") { history.next() }
            assertTrue(e1 is TrackEvent.Added && e1.track === inv.tracks[0], "$row: the history delivers the discovery first; got $e1")
            assertEquals(TrackEvent.CatalogReady, owed(row, "the catalog-ready event") { history.next() }, "$row: then the catalog-ready event")
            rig.endHistory(fatal, row, "after the prefix")
            rig.assertFinalInventory(fatal, row)
        }

    @Test
    fun `clean terminal -- objects, then SAP, then timeline each deliver their owed prefix before the end`() =
        terminalRow(fatal = false, order = listOf(Family.Objects, Family.Sap, Family.Timeline))

    @Test
    fun `clean terminal -- timeline, then SAP, then objects each deliver their owed prefix before the end`() =
        terminalRow(fatal = false, order = listOf(Family.Timeline, Family.Sap, Family.Objects))

    @Test
    fun `clean terminal -- SAP, then objects, then timeline each deliver their owed prefix before the end`() =
        terminalRow(fatal = false, order = listOf(Family.Sap, Family.Objects, Family.Timeline))

    @Test
    fun `fatal terminal -- objects, then SAP, then timeline each deliver their owed prefix before the receiver's own fatal code`() =
        terminalRow(fatal = true, order = listOf(Family.Objects, Family.Sap, Family.Timeline))

    @Test
    fun `fatal terminal -- timeline, then objects, then SAP each deliver their owed prefix before the receiver's own fatal code`() =
        terminalRow(fatal = true, order = listOf(Family.Timeline, Family.Objects, Family.Sap))

    @Test
    fun `reverse interleaving -- final track events queued while a data poll is held are integrated before that stream reports the terminal`() =
        runTest {
            val row = "R2 reverse"
            val rig = rig(WITH_HISTORY)
            val backend = rig.backend
            val receiver = rig.receiver
            backend.scriptTrackEvents(added(T1, VIDEO), ready)
            backend.markActivity()
            val track = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }.tracks.single()
            val history = receiver.trackEvents!!
            assertTrue(withTimeout(BOUND) { history.next() } is TrackEvent.Added, "$row: setup: the discovery event")
            assertEquals(TrackEvent.CatalogReady, withTimeout(BOUND) { history.next() }, "$row: setup: the catalog-ready event")
            assertEquals(0, backend.observed().trackPollsLeftQueued, "$row: setup: the track pre-drain of the coming demand answers Empty")

            backend.holdNextObjectPoll()
            val demand = async(Dispatchers.Default) { receiver.objects.next() }
            try {
                assertTrue(backend.awaitCondition("the data poll is held inside the call") { it.objectPollHeld }, "$row: the object poll was never held")
                // While the data poll is held: the final track events land and
                // the endpoint closes, which terminalizes the receiver. The
                // held poll then finds its own ring empty and answers CLOSED.
                backend.scriptTrackEvents(ended(T1))
                backend.pollTrackTerminal = BackendTrackEventResult.Closed(false, 0uL)
                backend.pollObjectTerminal = BackendObjectResult.Closed(false, 0uL)
                backend.snapshotResult = BackendSnapshotResult.Ok(CLOSED_ENDPOINT)
                backend.setClosed(true)
            } finally {
                backend.releaseObjectPoll()
            }
            assertEquals(null, withTimeout(BOUND) { demand.await() }, "$row: the held poll answered CLOSED on an empty object ring: the clean end")

            // By the time a stream reports the terminal, the track ring's final
            // events have been consumed and integrated.
            val seen = backend.observed()
            assertEquals(0, seen.trackPollsLeftQueued, "$row: the final track events queued before the terminal were left in the native track ring -- track integration stopped at the object ring's CLOSED")
            val inv = receiver.inventory.value
            assertEquals(TrackPresence.Ended, track.presence, "$row: the track's own ENDED was not integrated")
            assertEquals(ReceiverTerminal.Closed, inv.terminal, "$row: one receiver-wide terminal reason")
            val e = withTimeout(BOUND) { history.next() }
            assertTrue(e is TrackEvent.Ended && e.track === track, "$row: the history delivers the final ENDED before it closes; got $e")
            assertEquals(null, withTimeout(BOUND) { history.next() }, "$row: the history closes after its ring is exhausted")
            val polls = backend.observed().objectPolls.size
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "$row: the repeated end")
            assertEquals(polls, backend.observed().objectPolls.size, "$row: the repeated end polls nothing more")
        }

    @Test
    fun `a per-track ENDED and a record Deferral leave the receiver live -- neither is a receiver-wide terminal`() =
        runTest {
            val row = "R2 control/ended-and-deferral"
            val rig = rig()
            val backend = rig.backend
            val receiver = rig.receiver
            backend.scriptTrackEvents(added(T1, VIDEO), ready, ended(T1))
            backend.scriptObjects(O1)
            backend.sapDeferrals = 1uL
            backend.markActivity()
            val inv = withTimeout(BOUND) { receiver.inventory.first { it.tracks.singleOrNull()?.presence == TrackPresence.Ended } }
            assertEquals(null, inv.terminal, "$row: an ENDED track is not a terminal")
            val d = withTimeout(BOUND) { receiver.sapRecords.next() }
            assertEquals(ReceiverRecordEvent.Deferral(1uL, 0uL), d, "$row: the deferral witness is a value the stream continues past")
            assertObject(row, "the object", withTimeout(BOUND) { receiver.objects.next() }, O1, inv.tracks.single())
            assertEquals(null, receiver.inventory.value.terminal, "$row: still live after both")
            assertEquals(TrackState.Ended, withTimeout(BOUND) { receiver.trackState(inv.tracks.single()) }, "$row: the track itself reads Ended")
        }

    @Test
    fun `explicit receiver close stops native polling and a staged handoff is still delivered before the end`() =
        runTest {
            val row = "R2 control/explicit-close"
            val rig = rig()
            val backend = rig.backend
            val receiver = rig.receiver
            backend.scriptTrackEvents(added(T1, VIDEO), ready)
            backend.markActivity()
            val track = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }.tracks.single()
            backend.scriptObjects(STAGED, UNDELIVERED)
            backend.holdNextObjectPoll()
            val cancelled = async(Dispatchers.Default) { receiver.objects.next() }
            try {
                assertTrue(backend.awaitCondition("the poll is held") { it.objectPollHeld }, "$row: the object poll was never held")
                cancelled.cancel()
            } finally {
                backend.releaseObjectPoll()
            }
            runCatching { withTimeout(BOUND) { cancelled.await() } }
            withTimeout(BOUND) { receiver.close() }
            val polls = backend.observed().objectPolls.size
            assertEquals(1, polls, "$row: exactly the held poll ran")
            assertObject(row, "the staged handoff", withTimeout(BOUND) { receiver.objects.next() }, STAGED, track)
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "$row: then the end")
            assertEquals(polls, backend.observed().objectPolls.size, "$row: nothing polls after close")
            assertEquals(1, backend.observed().objectsLeftQueued, "$row: the undelivered object stays native and dies with the C receiver")
            assertEquals(1, backend.observed().receiverCloses.size, "$row: the child was destroyed exactly once")
        }
}
