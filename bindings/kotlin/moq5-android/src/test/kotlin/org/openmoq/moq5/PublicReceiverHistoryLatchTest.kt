package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.EngineFixture
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The latched exact-history failure through the public receiver: after the
 * first gap the history stays failed while the producer keeps integrating
 * -- later events still update the inventory, the object stream stays
 * healthy, and no later arrival is ever delivered by the failed history.
 */
internal class PublicReceiverHistoryLatchTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false, eventHistory = EventHistoryPolicy.Exact(capacity = 2))
        val T1 = -0x7fff_ffff_ffff_fefdL
        val T2 = -0x7fff_ffff_ffff_fdfdL
        val T3 = -0x7fff_ffff_ffff_fcfdL
        val T4 = -0x7fff_ffff_ffff_fbfdL
        fun desc(name: String) = TrackDescription(name = MediaBytes(name.toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        fun added(t: Long, name: String) = BackendTrackEvent(BackendTrackEventKind.Added, t, desc(name))
        fun removed(t: Long) = BackendTrackEvent(BackendTrackEventKind.Removed, t, null)
        fun raw(t: Long, bytes: String) =
            BackendObject(
                track = t,
                payload = MediaPayload.Raw(MediaBytes(bytes.toByteArray())),
                status = ObjectStatus.Normal,
                endOfGroup = false,
                viaDatagram = false,
                keyframe = false,
                captureTime = null,
                decodeTime = MediaMicros(0uL),
                compositionOffset = MediaOffsetMicros(0L),
                presentationTime = MediaMicros(0uL),
            )
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    @Test
    fun `after the first gap the history stays failed while the inventory and the object stream keep working`() =
        runTest {
            val backend = newBackend()
            val endpoint = ownEndpoint(MoqEndpoint.open(EndpointConfig(URL)) { backend }, backend)
            run {
                val receiver = endpoint.attachReceiver(CONFIG)
                val history = receiver.trackEvents!!
                backend.scriptTrackEvents(added(T1, "a"), added(T2, "b"), added(T3, "c")) // c is the gap
                backend.markActivity()
                val three = withTimeout(BOUND) { receiver.inventory.first { it.tracks.size == 3 } }
                val a = three.tracks[0]
                val e1 = withTimeout(BOUND) { history.next() }
                assertTrue(e1 is TrackEvent.Added && e1.track === a, "R7 public: the pre-gap prefix, a; got $e1")
                // A later arrival, while a history slot is free: it must update
                // the inventory and must never be delivered by the failed history.
                backend.scriptTrackEvents(added(T4, "d"))
                backend.scriptObjects(raw(T1, "o1"))
                backend.markActivity()
                val four = withTimeout(BOUND) { receiver.inventory.first { it.tracks.size == 4 } }
                assertEquals(listOf("a", "b", "c", "d"), four.tracks.map { String(it.description.name.bytes()) }, "R7 public: the producer kept integrating")
                assertEquals("o1", String(withTimeout(BOUND) { receiver.objects.next() }!!.mediaBytes().bytes()), "R7 public: the object stream is healthy")
                val e2 = withTimeout(BOUND) { history.next() }
                assertTrue(e2 is TrackEvent.Added && e2.track === three.tracks[1], "R7 public: the pre-gap prefix, b; got $e2")
                val overflow = assertFailsWith<TrackEventHistoryOverflowException>("R7 public: after the pre-gap prefix the history fails; d must never be delivered") { withTimeout(BOUND) { history.next() } }
                assertEquals(2uL, overflow.dropped, "R7 public: c and d were lost to the gap")
                // Still integrating, still failed.
                backend.scriptTrackEvents(removed(T1))
                backend.markActivity()
                withTimeout(BOUND) { receiver.inventory.first { it.tracks[0].presence == TrackPresence.Removed } }
                assertFailsWith<TrackEventHistoryOverflowException>("R7 public: the failure is latched; the removal is integrated but never delivered by the failed history") { withTimeout(BOUND) { history.next() } }
                assertEquals(null, receiver.inventory.value.terminal, "R7 public: a failed history is not a receiver terminal")
            }
            // Teardown and the violation audit are the fixture's, unconditional.
        }
}
