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
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

/**
 * The wire identity on the public [MediaObject]: exactly what the backend
 * delivered, per object, in delivery order; null when the backend carried
 * none, which is distinct from an all-zero identity and from an absent
 * subgroup. Identity is a property of the object, never of a sample.
 */
internal class PublicReceiverObjectIdentityTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val VIDEO = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        fun raw(bytes: String, identity: WireIdentity?) =
            BackendObject(
                track = T1, payload = MediaPayload.Raw(MediaBytes(bytes.toByteArray())), status = ObjectStatus.Normal,
                endOfGroup = false, viaDatagram = identity?.subgroup == null && identity != null, keyframe = false,
                captureTime = null, decodeTime = MediaMicros(1_000uL), compositionOffset = MediaOffsetMicros(0L),
                presentationTime = MediaMicros(1_000uL), identity = identity,
            )
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    @Test
    fun `each delivered object carries its own identity, or null when the backend had none`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            backend.scriptTrackEvents(BackendTrackEvent(BackendTrackEventKind.Added, T1, VIDEO), BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null))
            backend.markActivity()
            withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }
            val high = WireIdentity(group = ULong.MAX_VALUE, subgroup = 4uL, objectId = 0x8000_0000_0000_0003uL, publisherPriority = 17u)
            val zero = WireIdentity(0uL, 0uL, 0uL, 0u)
            val datagram = WireIdentity(group = 7uL, subgroup = null, objectId = 5uL, publisherPriority = 200u)
            backend.scriptObjects(raw("o1", high), raw("o2", zero), raw("o3", datagram), raw("o4", null))
            backend.markActivity()
            val got = List(4) { withTimeout(BOUND) { receiver.objects.next() }!! }
            assertEquals(listOf(high, zero, datagram, null), got.map { it.identity })
            assertEquals(listOf("o1", "o2", "o3", "o4"), got.map { String((it.payload as MediaPayload.Raw).payload.bytes()) })
            assertNull(got[3].identity)
            assertEquals(true, got[2].viaDatagram)
            endpoint.closeAndJoin()
        }
}
