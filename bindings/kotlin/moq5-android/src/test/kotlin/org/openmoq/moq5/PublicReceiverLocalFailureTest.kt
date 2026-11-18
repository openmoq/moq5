package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendDeliveryOrigin
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BackendTrackEventResult
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

/**
 * Local failures and fill outcomes on the public receiver, over the scripted
 * backend (`media_receiver.h`: track-event kinds 8-12 and
 * `MOQ_MEDIA_TRACK_STATE_FAILED`).
 *
 * Derived contract:
 *  * Each kind is a typed [TrackEvent] carrying exactly what the native event
 *    carried; a field it did not carry is null.
 *  * A track-scoped failure -- LOCAL_FAILURE, UPDATE_LOCAL_FAILURE, a LIVE
 *    DELIVERY_LOCAL_FAILURE -- makes the track [TrackPresence.Failed]. A fill
 *    child's failure or end, a fill's creation, and a delivery failure whose
 *    origin was not carried leave the track as it was.
 *  * Failure is isolated: the endpoint is not quarantined, other tracks keep
 *    delivering, and objects accepted before the failure stay deliverable.
 *  * A failed track refuses subscribe as [TrackUnavailableException] with
 *    [TrackPresence.Failed], still disables, and reads [TrackState.Failed] --
 *    also once the receiver is terminal, where every other track reads Ended.
 *    A failure with no event (credentials) is learned from the state read,
 *    never treated as a breach. A later end keeps it Failed; a removal wins.
 */
internal class PublicReceiverLocalFailureTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val MANUAL = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false, eventHistory = EventHistoryPolicy.Exact(32))
        const val T1 = 0x1101L
        const val T2 = 0x2202L
        val VIDEO = TrackDescription(name = MediaBytes("v".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        val AUDIO = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("audio".toByteArray()))
        fun ev(kind: BackendTrackEventKind, t: Long) = BackendTrackEvent(kind, t, null)
        val ready = ev(BackendTrackEventKind.CatalogReady, 0L)
        val NOMEM = MoqResultCode(-1)
        val BUFFER = MoqResultCode(-9)
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private class Session(val backend: ScriptedEndpointBackend, val endpoint: MoqEndpoint, val receiver: MediaReceiver, val video: MediaTrack, val audio: MediaTrack)

    private suspend fun session(): Session {
        val backend = newBackend()
        val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
        val receiver = endpoint.attachReceiver(MANUAL)
        backend.scriptTrackEvents(BackendTrackEvent(BackendTrackEventKind.Added, T1, VIDEO), BackendTrackEvent(BackendTrackEventKind.Added, T2, AUDIO), ready)
        backend.markActivity()
        val inv = withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
        return Session(backend, endpoint, receiver, inv.tracks[0], inv.tracks[1])
    }

    /** Script [events] and wait until the inventory has integrated all of them. */
    private suspend fun Session.deliver(vararg events: BackendTrackEvent): ReceiverInventory {
        val from = receiver.inventory.value.revision
        backend.scriptTrackEvents(*events)
        backend.markActivity()
        return withTimeout(BOUND) { receiver.inventory.first { it.revision >= from + events.size.toULong() } }
    }

    private suspend fun Session.history(n: Int): List<TrackEvent> {
        val stream = checkNotNull(receiver.trackEvents)
        return withTimeout(BOUND) { List(n) { checkNotNull(stream.next()) } }
    }

    private fun objectFor(t: Long, bytes: String) =
        BackendObject(
            track = t, payload = MediaPayload.Raw(MediaBytes(bytes.toByteArray())), status = ObjectStatus.Normal,
            endOfGroup = false, viaDatagram = false, keyframe = true, captureTime = null,
            decodeTime = MediaMicros(0uL), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(0uL),
        )

    @Test
    fun `each kind is a typed public event carrying exactly what the native event carried, and only track-scoped failures fail the track`() =
        runTest {
            val s = session()
            s.deliver(
                ev(BackendTrackEventKind.FillCreated, T2).copy(initiatingRequestId = RequestId(0uL)),
                ev(BackendTrackEventKind.FillTerminal, T2).copy(
                    initiatingRequestId = RequestId(0uL), fillReason = FillEndReason.PeerCancelledOrReset,
                    fillErrorDomain = FillErrorDomain.Stream, fillErrorCode = MoqErrorCode(0uL), fillLoss = FillLoss.None,
                ),
                ev(BackendTrackEventKind.DeliveryLocalFailure, T2).copy(localResult = BUFFER, deliveryOrigin = BackendDeliveryOrigin.Fill, deliveryInitiatingRequestId = RequestId(0uL)),
                ev(BackendTrackEventKind.DeliveryLocalFailure, T2).copy(localResult = BUFFER),
            )
            assertEquals(TrackPresence.Present, s.audio.presence, "fill outcomes, a fill child's delivery failure and an uncarried origin leave the track present")
            s.deliver(ev(BackendTrackEventKind.LocalFailure, T1).copy(localResult = NOMEM), ev(BackendTrackEventKind.UpdateLocalFailure, T2))
            assertEquals(TrackPresence.Failed, s.video.presence)
            assertEquals(TrackPresence.Failed, s.audio.presence)
            assertEquals(
                listOf(
                    TrackEvent.Added(s.video), TrackEvent.Added(s.audio), TrackEvent.CatalogReady,
                    TrackEvent.FillCreated(s.audio, RequestId(0uL)),
                    TrackEvent.FillTerminal(s.audio, RequestId(0uL), FillEndReason.PeerCancelledOrReset, FillErrorDomain.Stream, MoqErrorCode(0uL), FillLoss.None),
                    TrackEvent.DeliveryLocalFailure(s.audio, BUFFER, DeliveryOrigin.Fill(RequestId(0uL))),
                    TrackEvent.DeliveryLocalFailure(s.audio, BUFFER, null),
                    TrackEvent.LocalFailure(s.video, NOMEM),
                    TrackEvent.UpdateLocalFailure(s.audio, null),
                ),
                s.history(9),
            )
            assertEquals(0, s.receiver.inventory.value.anomalies)
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `a LIVE delivery failure fails only that track -- its accepted objects and the other track keep flowing, with no quarantine`() =
        runTest {
            val s = session()
            s.receiver.subscribe(s.video)
            s.receiver.subscribe(s.audio)
            s.backend.acceptSubscription(T1)
            s.backend.acceptSubscription(T2)
            s.backend.scriptObjects(objectFor(T1, "v1"))
            s.deliver(ev(BackendTrackEventKind.DeliveryLocalFailure, T1).copy(localResult = BUFFER, deliveryOrigin = BackendDeliveryOrigin.Live))
            s.backend.scriptObjects(objectFor(T2, "a1"))
            assertEquals(TrackPresence.Failed, s.video.presence)
            assertEquals(TrackPresence.Present, s.audio.presence)
            assertEquals(listOf("v1", "a1"), List(2) { String(withTimeout(BOUND) { s.receiver.objects.next() }!!.mediaBytes().bytes()) })
            assertEquals(TrackState.Active, withTimeout(BOUND) { s.receiver.trackState(s.audio) }, "the endpoint still answers")
            assertEquals(TrackEvent.DeliveryLocalFailure(s.video, BUFFER, DeliveryOrigin.Live), s.history(4).last())
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `a failed track refuses subscribe by name without a backend call, still disables, and reads Failed`() =
        runTest {
            val s = session()
            s.receiver.subscribe(s.video)
            s.deliver(ev(BackendTrackEventKind.LocalFailure, T1).copy(localResult = NOMEM))
            assertEquals(TrackState.Failed, withTimeout(BOUND) { s.receiver.trackState(s.video) })
            s.receiver.unsubscribe(s.video)
            assertEquals(listOf(T1), s.backend.observed().unsubscribeCalls.map { it.track }, "disable reaches the C")
            val refused = assertFailsWith<TrackUnavailableException> { s.receiver.subscribe(s.video) }
            assertEquals(TrackPresence.Failed, refused.presence)
            assertEquals(1, s.backend.observed().subscribeCalls.size, "refused locally: no second subscribe")
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `a failure with no event is learned from the track's state -- the refusal is named and nothing is quarantined`() =
        runTest {
            val s = session()
            s.backend.failTrackSilently(T1)
            val refused = assertFailsWith<TrackUnavailableException> { s.receiver.subscribe(s.video) }
            assertEquals(TrackPresence.Failed, refused.presence, "WRONG_STATE explained by the FAILED state read")
            assertEquals(TrackPresence.Failed, s.video.presence, "and folded into the track")
            s.receiver.subscribe(s.audio)
            assertEquals(listOf(T1, T2), s.backend.observed().subscribeCalls.map { it.track }, "the endpoint still takes commands")
            s.backend.failTrackSilently(T2)
            assertEquals(TrackState.Failed, withTimeout(BOUND) { s.receiver.trackState(s.audio) })
            assertEquals(TrackPresence.Failed, s.audio.presence, "a FAILED read is folded in too")
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `a terminal receiver keeps a failed track Failed and reads every other track Ended`() =
        runTest {
            val s = session()
            s.deliver(ev(BackendTrackEventKind.LocalFailure, T1).copy(localResult = NOMEM))
            s.backend.pollTrackTerminal = BackendTrackEventResult.Closed(fatal = false, code = 0uL)
            s.backend.markActivity()
            withTimeout(BOUND) { s.receiver.inventory.first { it.terminal != null } }
            assertEquals(TrackState.Failed, withTimeout(BOUND) { s.receiver.trackState(s.video) })
            assertEquals(TrackState.Ended, withTimeout(BOUND) { s.receiver.trackState(s.audio) })
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `a track that failed with no event, refusal or state read still reads Failed once the receiver is terminal`() =
        runTest {
            val s = session()
            s.backend.failTrackSilently(T1)
            s.backend.pollTrackTerminal = BackendTrackEventResult.Closed(fatal = false, code = 0uL)
            s.backend.markActivity()
            withTimeout(BOUND) { s.receiver.inventory.first { it.terminal != null } }
            assertEquals(TrackPresence.Present, s.video.presence, "nothing has told the binding yet")
            assertEquals(TrackState.Failed, withTimeout(BOUND) { s.receiver.trackState(s.video) }, "the C keeps FAILED past the terminal")
            assertEquals(TrackPresence.Failed, s.video.presence, "and the read folds it in")
            assertEquals(TrackState.Ended, withTimeout(BOUND) { s.receiver.trackState(s.audio) }, "a healthy track of a terminal receiver reads Ended")
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `a failed track that later ends stays Failed, and a catalog removal replaces it`() =
        runTest {
            val s = session()
            s.deliver(ev(BackendTrackEventKind.LocalFailure, T1), ev(BackendTrackEventKind.Ended, T1))
            assertEquals(TrackPresence.Failed, s.video.presence)
            assertEquals(TrackEvent.Ended(s.video), s.history(5).last(), "the end is still reported")
            s.deliver(ev(BackendTrackEventKind.Removed, T1))
            assertEquals(TrackPresence.Removed, s.video.presence)
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `a local kind for a handle never announced is counted and dropped`() =
        runTest {
            val s = session()
            val inv = s.deliver(ev(BackendTrackEventKind.FillTerminal, 0x9909L), ev(BackendTrackEventKind.LocalFailure, 0x9909L))
            assertEquals(2, inv.anomalies)
            assertEquals(listOf(TrackPresence.Present, TrackPresence.Present), inv.tracks.map { it.presence })
            s.endpoint.closeAndJoin()
        }

    @Test
    fun `the binding mirrors exactly the track-event kinds and track states the C header defines`() {
        val header = File("../../../service/include/moq/media_receiver.h").readText()
        fun enumBody(name: String): String =
            Regex("""typedef enum $name \{(.*?)\} $name""" + "_t;", RegexOption.DOT_MATCHES_ALL).find(header)?.groupValues?.get(1)
                ?: error("the header has no enum $name")
        fun values(body: String, prefix: String): List<Int> {
            var next = 0
            return Regex("""^\s*($prefix[A-Z_]+)\s*(?:=\s*(\d+))?\s*,?""", RegexOption.MULTILINE).findAll(body).map { m ->
                val v = m.groupValues[2].takeIf { it.isNotEmpty() }?.toInt() ?: next
                next = v + 1
                v
            }.toList()
        }
        // The members are MOQ_MEDIA_TRACK_* plus MOQ_MEDIA_CATALOG_READY.
        val kinds = values(enumBody("moq_media_track_event_kind"), "MOQ_MEDIA_(?:TRACK_|CATALOG_)")
        assertEquals((1..12).toList(), kinds, "the header's kinds as parsed; a new kind must be mirrored")
        assertEquals(kinds, org.openmoq.moq5.engine.BackendTrackEventKind.entries.map { it.native }, "every header kind has a binding kind, in order")
        val states = values(enumBody("moq_media_track_state"), "MOQ_MEDIA_TRACK_STATE_")
        assertEquals(states, TrackState.entries.map { it.ordinal }, "every header state has a TrackState")
        for (raw in states) assertEquals(TrackState.entries[raw], TrackState.ofNative(raw))
        assertNull(runCatching { TrackState.ofNative(states.max() + 1) }.getOrNull(), "past the header: refused")
    }
}
