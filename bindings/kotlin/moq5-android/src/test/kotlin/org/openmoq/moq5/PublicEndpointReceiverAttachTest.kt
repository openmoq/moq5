package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendSnapshotResult
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The receiver ATTACH lifecycle on the public endpoint, RED-first and over
 * the scripted backend -- the smallest receiver slice, before any event,
 * object or track control: it proves the child-handle model and the C
 * teardown order (`media_receiver.h`, `endpoint.c`).
 *
 * Derived contract:
 *  * `attachReceiver(config)` borrows the endpoint (`moq_media_receiver_attach`,
 *    cfg.endpoint NULL): at most ONE receiver per endpoint (a second attach is
 *    WRONG_STATE at the C tier); the attach runs on the endpoint's service
 *    worker, the authority thread of every stateful row.
 *  * `ReceiverConfig` mirrors `moq_media_receiver_cfg_t` (plan §5f): the
 *    namespace (required), the overflow policy (REQUIRED, no default -- the C
 *    tier fails INVAL when unset), object/byte bounds, the track-event bound,
 *    auto-subscribe, the catalog track override, and the time mode.
 *  * `moq_endpoint_stop()` REFUSES with WRONG_STATE while a receiver is
 *    attached, so the endpoint's teardown destroys its receiver BEFORE stop;
 *    a receiver closed by the app is destroyed once, and never again at
 *    endpoint teardown.
 *  * After a logical close, attach refuses with `EndpointClosedException`;
 *    after a bridge quarantine, with the stable `BridgeContractException`;
 *    neither makes a native call.
 */
internal class PublicEndpointReceiverAttachTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = kotlin.time.Duration.parse("5s")
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray()), NamespacePart("cam1".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe)
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    @Test
    fun `ReceiverConfig mirrors the C config, with the overflow policy a required choice and the C defaults for the rest`() {
        assertEquals(NAMESPACE, CONFIG.namespace)
        assertEquals(OverflowPolicy.DropToKeyframe, CONFIG.overflow)
        assertEquals(256u, CONFIG.maxObjects)
        assertEquals(32uL * 1024u * 1024u, CONFIG.maxBytes)
        assertEquals(64u, CONFIG.maxTrackEvents)
        assertEquals(true, CONFIG.autoSubscribe)
        assertEquals(null, CONFIG.catalogTrack)
        assertEquals(TimeMode.Raw, CONFIG.timeMode)
        assertEquals(setOf("DropToKeyframe", "DropGroup", "FlowControl"), OverflowPolicy.entries.map { it.name }.toSet(), "no UNSET is representable")
    }

    @Test
    fun `attachReceiver attaches once, on the service worker, with the exact config`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            val seen = backend.observed()
            assertEquals(1, seen.receiverAttaches.size)
            assertEquals(CONFIG, seen.receiverAttaches.single().config)
            assertEquals(seen.serviceThreadId, seen.receiverAttaches.single().threadId, "attach runs on the endpoint's worker")
            assertTrue(!receiver.isClosed)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a second attach is refused by the C tier and surfaces as the exact native refusal`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.attachReceiver(CONFIG)
            backend.attachResult = org.openmoq.moq5.engine.BackendAttachResult.Refused(MoqResultCode(-5))
            val thrown = assertFailsWith<ReceiverAttachFailedException> { endpoint.attachReceiver(CONFIG) }
            assertEquals(MoqResultCode(-5), thrown.code)
            endpoint.closeAndJoin()
        }

    @Test
    fun `endpoint teardown destroys an attached receiver before stop, exactly once`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            endpoint.closeAndJoin()
            val order = backend.observed().callOrder.filter { it == "receiver-destroy" || it == "stop" || it == "destroy" }
            assertEquals(listOf("receiver-destroy", "stop", "destroy"), order, "the C stop refuses while a receiver is attached")
            assertTrue(receiver.isClosed)
        }

    @Test
    fun `a receiver closed by the app is destroyed once and never again at endpoint teardown`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            receiver.close()
            receiver.close() // idempotent
            assertTrue(receiver.isClosed)
            endpoint.closeAndJoin()
            val order = backend.observed().callOrder.filter { it == "receiver-destroy" || it == "stop" || it == "destroy" }
            assertEquals(listOf("receiver-destroy", "stop", "destroy"), order)
        }

    @Test
    fun `a closed receiver frees the slot, and a later attach on the live endpoint attaches a new receiver`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val first = endpoint.attachReceiver(CONFIG)
            first.close()
            assertTrue(first.isClosed)
            val second = endpoint.attachReceiver(CONFIG)
            assertTrue(!second.isClosed)
            assertEquals(2, backend.observed().receiverAttaches.size)
            assertTrue(backend.observed().receiverAttaches[0].token != backend.observed().receiverAttaches[1].token, "a new child, never the old token")
            endpoint.closeAndJoin()
            assertEquals(listOf("receiver-destroy", "receiver-destroy", "stop", "destroy"),
                backend.observed().callOrder.filter { it == "receiver-destroy" || it == "stop" || it == "destroy" })
            assertTrue(second.isClosed)
            assertTrue(backend.observed().violations.isEmpty(), backend.observed().violations.toString())
        }

    @Test
    fun `an empty receiver namespace, an empty part, an empty catalog track and unrepresentable bounds are refused before any backend work`() =
        runTest {
            assertFailsWith<IllegalArgumentException> { ReceiverConfig(Namespace(emptyList()), OverflowPolicy.DropGroup) }
            assertFailsWith<IllegalArgumentException> {
                ReceiverConfig(Namespace(listOf(NamespacePart("live".toByteArray()), NamespacePart(ByteArray(0)))), OverflowPolicy.DropGroup)
            }
            assertFailsWith<IllegalArgumentException> { ReceiverConfig(NAMESPACE, OverflowPolicy.DropGroup, catalogTrack = TrackName(ByteArray(0))) }
            assertEquals(TrackName("cat".toByteArray()), ReceiverConfig(NAMESPACE, OverflowPolicy.DropGroup, catalogTrack = TrackName("cat".toByteArray())).catalogTrack, "the catalog track is a TRACK NAME, not a namespace part")
            assertFailsWith<IllegalArgumentException> { ReceiverConfig(NAMESPACE, OverflowPolicy.DropGroup, maxObjects = Int.MAX_VALUE.toUInt() + 1u) }
            assertFailsWith<IllegalArgumentException> { ReceiverConfig(NAMESPACE, OverflowPolicy.DropGroup, maxTrackEvents = UInt.MAX_VALUE) }
            assertFailsWith<IllegalArgumentException> { ReceiverConfig(NAMESPACE, OverflowPolicy.DropGroup, maxBytes = Long.MAX_VALUE.toULong() + 1u) }
            // The largest representable bounds are accepted.
            ReceiverConfig(NAMESPACE, OverflowPolicy.DropGroup, maxObjects = Int.MAX_VALUE.toUInt(), maxTrackEvents = Int.MAX_VALUE.toUInt(), maxBytes = Long.MAX_VALUE.toULong())
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            assertTrue(backend.observed().receiverAttaches.isEmpty(), "no invalid config reached the backend")
            endpoint.closeAndJoin()
        }

    @Test
    fun `after close, attach refuses with EndpointClosedException and makes no native call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            endpoint.close()
            // Bounded: a refusal that is not immediate would strand the job
            // behind a retiring worker, and the row must fail by name, not hang.
            assertFailsWith<EndpointClosedException> { withTimeout(BOUND) { endpoint.attachReceiver(CONFIG) } }
            endpoint.closeAndJoin()
            assertTrue(backend.observed().receiverAttaches.isEmpty())
        }

    @Test
    fun `after a bridge quarantine, attach reports the stable failure and makes no native call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            backend.snapshotResult = BackendSnapshotResult.Bridge(BridgeFailure("snapshot", -1002))
            val first = assertFailsWith<BridgeContractException> { endpoint.snapshot() }
            val second = assertFailsWith<BridgeContractException> { endpoint.attachReceiver(CONFIG) }
            assertEquals(first.failure, second.failure)
            assertTrue(backend.observed().receiverAttaches.isEmpty())
            runCatching { endpoint.closeAndJoin() }
        }
}
