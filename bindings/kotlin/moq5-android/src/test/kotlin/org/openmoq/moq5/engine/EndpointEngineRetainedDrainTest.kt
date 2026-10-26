package org.openmoq.moq5.engine

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.OverflowPolicy
import org.openmoq.moq5.ReceiverConfig
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Retained-work progress after a native drain is MECHANICS, not
 * observability: an engine that publishes no snapshot records must still
 * re-attempt retained demand on the observation that follows a drain which
 * consumed the endpoint's only genuine activity. The public endpoint always
 * publishes, so this pins the engine's own contract directly.
 */
internal class EndpointEngineRetainedDrainTest : EngineFixture() {
    private companion object {
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val RX = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe, autoSubscribe = false)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val VIDEO = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        val CAPTURE = UnixMicros(1_700_000_000_000_009uL)
        val DATA =
            BackendObject(
                track = T1, payload = MediaPayload.Raw(MediaBytes("data".toByteArray())), status = ObjectStatus.Normal, endOfGroup = false,
                viaDatagram = false, keyframe = true, captureTime = CAPTURE,
                decodeTime = MediaMicros(CAPTURE.value), compositionOffset = MediaOffsetMicros(0L), presentationTime = MediaMicros(CAPTURE.value),
            )
    }

    @Test
    fun `without snapshot publication, a retained demand is still re-attempted after a drain consumed its activity, with no snapshot call`() =
        runBlocking {
            val (engine, backend) = newEngine() // publishSnapshots = false: the engine's default
            engine.awaitConstructed()
            val attached = engine.attachReceiver(RX)
            backend.scriptTrackEvents(BackendTrackEvent(BackendTrackEventKind.Added, T1, VIDEO), BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null))
            backend.markActivity()
            withTimeout(BOUND) { attached.model.inventory.first { it.catalogReady } }
            val stream = ObjectStream(engine, attached.token, attached.model)
            val consumer = async(Dispatchers.Default) { stream.next() }
            assertTrue(backend.awaitCondition("the demand answered Empty and is retained") { it.objectPolls.size == 1 && it.waitParked && !it.activityLevel }, "the demand never parked retained")
            backend.drainConsumesActivity = true
            backend.holdDrain()
            val drainer = async(Dispatchers.Default) { engine.drain(1_000_000L) }
            var released = false
            val pollsAtDrain: Int
            try {
                // The engine's own drain wake makes the worker leave its wait:
                // that pass costs exactly one confirmation poll (Empty) before
                // the worker parks for the drain. The oracle is the attempt
                // AFTER the drain, so the baseline is taken once both facts hold.
                assertTrue(backend.awaitCondition("the native drain is in flight and the drain wake's confirmation poll is done") { it.drainInFlight && it.objectPolls.size == 2 }, "the drain never started, or its wake's confirmation poll never ran")
                pollsAtDrain = backend.observed().objectPolls.size
                backend.scriptObjects(DATA)
                backend.markActivity() // the only genuine activity carrying the data; the drain will consume it
                assertTrue(backend.observed().activityLevel)
            } finally {
                backend.releaseDrain()
                released = true
            }
            assertTrue(released)
            assertEquals(BackendDrainResult.Flushed, withTimeout(BOUND) { drainer.await() })
            assertEquals(1, backend.observed().drainConsumedLevels, "the drain consumed the level itself")
            val delivered = backend.awaitCondition("the retained demand is re-attempted on the post-drain observation") { it.objectPolls.size == pollsAtDrain + 1 }
            val s = backend.observed()
            assertTrue(delivered, "F3: the drain consumed the only activity and the engine (publication off) never re-attempted the retained demand: polls=${s.objectPolls.size} (was $pollsAtDrain when the drain began), level=${s.activityLevel}, drainConsumedLevels=${s.drainConsumedLevels}")
            val o = withTimeout(BOUND) { consumer.await() }
            assertTrue(o != null && o.payload == DATA.payload, "the object is delivered; got $o")
            assertEquals(0, s.callOrder.count { it == "snapshot" }, "no snapshot record was taken: publication is off")
            engine.close()
        }
}
