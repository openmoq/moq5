package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendObjectResult
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.BridgeContractException
import org.openmoq.moq5.engine.BridgeFailure
import org.openmoq.moq5.engine.EngineFixture
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * The receiver OBJECT stream, RED-first over the scripted backend
 * (`media_receiver.h` §6.3: `moq_media_receiver_poll_object` transfers
 * exclusive ownership of the object's buffers; `MOQ_ERR_INTERRUPTED` while
 * the endpoint latch is set, BEFORE the queue is read; `MOQ_ERR_CLOSED` when
 * empty AND terminal; an object is never pollable before its track's ADDED).
 *
 * Derived contract:
 *  * PULL ONLY. Nothing polls objects on a service pass. One `next()` is one
 *    demand: one `poll_object` attempt on the service worker, preceded by the
 *    ordinary track-event drain so the object's track is already in the
 *    inventory. No JVM object queue exists by default.
 *  * A delivered [MediaObject] references the exact [MediaTrack] instance the
 *    inventory holds. The C queues a track's ADDED before any object for it
 *    but may do so after the demand's drain, so an object for a track the
 *    inventory does not know yet makes the demand drain the track events
 *    AGAIN; only a handle still unknown after that is a BRIDGE CONTRACT
 *    failure: the endpoint is quarantined and no track is invented.
 *  * Bytes are copied on the worker and the C object is cleaned exactly once
 *    there; the Kotlin object owns nothing native (v1, copied by design).
 *  * `MOQ_ERR_INTERRUPTED` is its own typed outcome, [ReceiverInterruptedException]:
 *    non-terminal, and it beats queued objects, because the C poll is gated
 *    on the latch before it reads the queue. `resume()` lets the same objects
 *    through.
 *  * A terminal is delivered AFTER the queued objects: clean -> `null`, fatal
 *    -> [ReceiverFatalException] with the receiver's own code domain.
 *  * One consumer at a time. A consumer cancelled AFTER its poll was claimed
 *    does not lose the object: the worker stages it in the receiver's
 *    one-object handoff slot, and the next demand takes it before polling.
 *  * Receiver close ends the stream; undelivered C-queued objects die with
 *    the C receiver and nothing polls after close.
 *  * The stream is an [ExactStream] in the general sense only -- one
 *    consumer, `next()` until `null` or a failure -- there is no staged
 *    history to overflow: what the consumer has not pulled sits in the C
 *    receiver under its own overflow policy.
 */
internal class PublicReceiverObjectTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val T2 = -0x7fff_ffff_ffff_fdfdL
        val VIDEO = TrackDescription(name = MediaBytes("a".toByteArray()), packaging = MediaBytes("loc".toByteArray()), role = MediaBytes("video".toByteArray()))
        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)
        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)
        fun raw(t: Long, bytes: String, keyframe: Boolean = false, status: ObjectStatus = ObjectStatus.Normal) =
            BackendObject(
                track = t,
                payload = MediaPayload.Raw(MediaBytes(bytes.toByteArray())),
                status = status,
                endOfGroup = false,
                viaDatagram = false,
                keyframe = keyframe,
                captureTime = null,
                decodeTime = MediaMicros(1_000uL),
                compositionOffset = MediaOffsetMicros(-33_000L),
                presentationTime = MediaMicros(1_033uL),
            )
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun discovered(backend: org.openmoq.moq5.engine.ScriptedEndpointBackend, receiver: MediaReceiver): MediaTrack {
        backend.scriptTrackEvents(added(T1, VIDEO), ready)
        backend.markActivity()
        return withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }.tracks[0]
    }

    @Test
    fun `objects are pulled on demand only -- passes drain events, never objects`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "o1"), raw(T1, "o2"), raw(T1, "o3"))
            backend.markActivity()
            backend.markActivity()
            assertTrue(backend.awaitCondition("a second pass ran") { it.waitEntries >= 3 })
            assertEquals(0, backend.observed().objectPolls.size, "no demand, no poll")
            assertEquals(3, backend.observed().objectsLeftQueued, "the native queue is the only queue")
            val o = withTimeout(BOUND) { receiver.objects.next() }
            assertEquals(MediaPayload.Raw(MediaBytes("o1".toByteArray())), o!!.payload)
            assertEquals(1, backend.observed().objectPolls.size, "one demand is exactly one poll")
            assertEquals(2, backend.observed().objectsLeftQueued)
            assertEquals(backend.observed().serviceThreadId, backend.observed().objectPolls[0].threadId, "polled on the worker")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a delivered object references the inventory's MediaTrack instance and carries its metadata in the right domains`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            val track = discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "key", keyframe = true, status = ObjectStatus.EndOfGroup))
            val o = withTimeout(BOUND) { receiver.objects.next() }!!
            assertTrue(o.track === track, "the same handle is the same instance")
            assertTrue(o.keyframe)
            assertEquals(ObjectStatus.EndOfGroup, o.status)
            assertEquals(null, o.captureTime, "absent capture time is null, never zero")
            assertEquals(MediaMicros(1_000uL), o.decodeTime)
            assertEquals(MediaOffsetMicros(-33_000L), o.compositionOffset, "the composition offset keeps its sign")
            assertEquals(MediaMicros(1_033uL), o.presentationTime)
            endpoint.closeAndJoin()
        }

    @Test
    fun `when an ADDED and its object are queued together, the demand drains the event first and the object names the discovered track`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            // Both land between passes; the demand arrives before the next pass.
            backend.scriptTrackEvents(added(T2, VIDEO.copy(name = MediaBytes("b".toByteArray()))))
            backend.scriptObjects(raw(T2, "first-of-b"))
            val before = backend.observed().callOrder.size
            val o = withTimeout(BOUND) { receiver.objects.next() }!!
            val demand = backend.observed().callOrder.drop(before)
            val poll = demand.indexOf("receiver-poll-object")
            val drain = demand.indexOf("receiver-poll-track")
            assertTrue(drain in 0 until poll, "the track drain precedes the object poll within the demand; saw $demand")
            assertTrue(o.track === receiver.inventory.value.tracks[1], "the object's track is the inventory's instance")
            assertEquals(MediaBytes("b".toByteArray()), o.track.description.name)
            endpoint.closeAndJoin()
        }

    @Test
    fun `an ADDED and its object queued while the worker is inside an object poll are delivered by the next demand, which drains again before deciding`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            // Demand 1 is held inside its (empty) poll: the worker is busy in a job.
            backend.holdNextObjectPoll()
            val first = async(Dispatchers.Default) { receiver.objects.next() }
            val second: kotlinx.coroutines.Deferred<MediaObject?>
            try {
                assertTrue(backend.awaitCondition("demand 1 is held inside its poll") { it.objectPollHeld })
                // Both land while the worker is busy; demand 2 is queued behind demand 1,
                // so the worker claims it with no wait -- and no pass -- in between.
                backend.scriptTrackEvents(added(T2, VIDEO.copy(name = MediaBytes("b".toByteArray()))))
                backend.scriptObjects(raw(T2, "first-of-b"))
                first.cancel()
                runCatching { withTimeout(BOUND) { first.await() } }
                second = async(Dispatchers.Default) { receiver.objects.next() }
                assertTrue(backend.awaitCondition("demand 2 is queued behind the held demand") { it.wakeCount >= 2 })
            } finally {
                backend.releaseObjectPoll()
            }
            val o = withTimeout(BOUND) { second.await() }!!
            assertEquals(MediaBytes("b".toByteArray()), o.track.description.name, "the object for b delivers, with b discovered")
            assertTrue(o.track === receiver.inventory.value.tracks[1])
            // The held poll itself dequeued the new object; the demand drained
            // AGAIN after it to discover b, staged the object, and the next
            // demand took it from the handoff slot with no second native poll.
            val order = backend.observed().callOrder
            val polls = order.withIndex().filter { it.value == "receiver-poll-object" }.map { it.index }
            assertEquals(1, polls.size, "one object poll; the handoff answered the next demand; saw $order")
            assertTrue(order.subList(polls[0] + 1, order.size).contains("receiver-poll-track"), "a track drain follows the poll, before the object is decided; saw $order")
            endpoint.closeAndJoin()
        }

    @Test
    fun `an object for a track no ADDED ever announces is a bridge contract failure after the second drain -- quarantine, no invented track`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T2, "orphan"))
            val drains = backend.observed().trackPolls.size
            val failure = assertFailsWith<BridgeContractException> { withTimeout(BOUND) { receiver.objects.next() } }
            assertEquals("receiver-object-track", failure.failure.op)
            assertTrue(backend.observed().trackPolls.size >= drains + 2, "the demand drained before the poll AND again after the unknown handle; saw ${backend.observed().trackPolls.size - drains} drains")
            assertEquals(1, receiver.inventory.value.tracks.size, "no track was invented")
            val quarantined = runCatching { withTimeout(BOUND) { endpoint.awaitEstablished(BOUND) } }.exceptionOrNull()
            assertTrue(quarantined is BridgeContractException, "the endpoint is quarantined; saw $quarantined")
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }

    @Test
    fun `the object stream admits one consumer at a time`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            val first = async(Dispatchers.Default) { receiver.objects.next() }
            assertTrue(backend.awaitCondition("the first consumer polled once and parked") { it.objectPolls.size == 1 })
            assertFailsWith<IllegalStateException> { receiver.objects.next() }
            backend.scriptObjects(raw(T1, "late"))
            backend.markActivity()
            assertEquals(MediaPayload.Raw(MediaBytes("late".toByteArray())), withTimeout(BOUND) { first.await() }!!.payload)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a parked consumer re-polls only when the worker observes activity, and the empty answer never spins`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            val consumer = async(Dispatchers.Default) { receiver.objects.next() }
            assertTrue(backend.awaitCondition("parked after one empty poll") { it.objectPolls.size == 1 && it.waitParked })
            val polls = backend.observed().objectPolls.size
            backend.scriptObjects(raw(T1, "o"))
            backend.markActivity()
            assertEquals(MediaPayload.Raw(MediaBytes("o".toByteArray())), withTimeout(BOUND) { consumer.await() }!!.payload)
            assertEquals(polls + 1, backend.observed().objectPolls.size, "one activity, one re-poll")
            endpoint.closeAndJoin()
        }

    @Test
    fun `the terminal is delivered after the queued objects -- clean ends with null`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "o1"), raw(T1, "o2"))
            backend.pollObjectTerminal = BackendObjectResult.Closed(fatal = false, code = 0uL)
            assertEquals("o1", String(withTimeout(BOUND) { receiver.objects.next() }!!.mediaBytes().bytes()))
            assertEquals("o2", String(withTimeout(BOUND) { receiver.objects.next() }!!.mediaBytes().bytes()))
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "clean terminal after the queue drained")
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "and it stays ended")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a fatal receiver terminal throws ReceiverFatalException with the receiver's own code, after the queued objects`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "last"))
            backend.pollObjectTerminal = BackendObjectResult.Closed(fatal = true, code = 0x2uL)
            assertEquals("last", String(withTimeout(BOUND) { receiver.objects.next() }!!.mediaBytes().bytes()))
            val fatal = assertFailsWith<ReceiverFatalException> { withTimeout(BOUND) { receiver.objects.next() } }
            assertEquals(ReceiverFatalCode.EVENT_OVERFLOW, fatal.code)
            assertEquals(ReceiverTerminal.Fatal(ReceiverFatalCode.EVENT_OVERFLOW), receiver.inventory.value.terminal, "the inventory learned the same terminal")
            endpoint.closeAndJoin()
        }

    @Test
    fun `an interrupted poll is its own typed outcome, beats queued objects, consumes none, and resume lets them through`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "held"))
            endpoint.interrupt()
            assertFailsWith<ReceiverInterruptedException> { withTimeout(BOUND) { receiver.objects.next() } }
            assertEquals(1, backend.observed().objectsLeftQueued, "the latch beat the queue; nothing was consumed")
            assertEquals(null, receiver.inventory.value.terminal, "not a terminal")
            endpoint.resume()
            assertEquals("held", String(withTimeout(BOUND) { receiver.objects.next() }!!.mediaBytes().bytes()))
            endpoint.closeAndJoin()
        }

    @Test
    fun `a consumer cancelled after its poll was claimed does not lose the object -- the next demand takes it from the handoff slot`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "racing"))
            backend.holdNextObjectPoll()
            val cancelled = async(Dispatchers.Default) { receiver.objects.next() }
            try {
                assertTrue(backend.awaitCondition("the poll is in flight and held") { it.objectPollHeld })
                cancelled.cancel()
            } finally {
                backend.releaseObjectPoll()
            }
            runCatching { withTimeout(BOUND) { cancelled.await() } }
            val o = withTimeout(BOUND) { receiver.objects.next() }!!
            assertEquals("racing", String(o.mediaBytes().bytes()), "the staged object, not a second poll's")
            assertEquals(1, backend.observed().objectPolls.size, "no second native poll: the handoff slot answered")
            endpoint.closeAndJoin()
        }

    @Test
    fun `receiver close ends the stream and nothing polls objects after it`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.scriptObjects(raw(T1, "undelivered"))
            receiver.close()
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "closed: the stream ends")
            assertEquals(0, backend.observed().objectPolls.size, "nothing polled after close")
            endpoint.closeAndJoin()
        }

    @Test
    fun `CMAF objects keep the full fragment, the mdat range and the samples, and RAW objects keep the payload`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            val fragment = MediaBytes("moof....mdatMEDIA".toByteArray())
            val cmaf = raw(T1, "").copy(payload = MediaPayload.Cmaf(fragment = fragment, mdat = ByteRange(offset = 12, length = 5), samples = listOf(CmafSample(durationTicks = 3000u, sizeBytes = 5u, flags = 0u, compositionOffsetTicks = -1))))
            backend.scriptObjects(cmaf, raw(T1, "rawbytes"))
            val c = withTimeout(BOUND) { receiver.objects.next() }!!
            val cp = c.payload as MediaPayload.Cmaf
            assertEquals(fragment, cp.fragment)
            assertEquals("MEDIA", String(c.mediaBytes().bytes()), "the media bytes of a CMAF object are the mdat slice, never an empty payload")
            assertEquals(1, cp.samples.size)
            assertEquals(ByteRange(12, 5), cp.mdat, "an explicit offset/length span, never an inclusive range")
            assertEquals(-1, cp.samples[0].compositionOffsetTicks)
            val r = withTimeout(BOUND) { receiver.objects.next() }!!
            assertEquals("rawbytes", String(r.mediaBytes().bytes()))
            assertTrue(r.payload is MediaPayload.Raw)
            endpoint.closeAndJoin()
        }

    @Test
    fun `a bridge answer on the object poll quarantines the endpoint and is not a receiver terminal`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)
            backend.pollObjectTerminal = BackendObjectResult.Bridge(BridgeFailure("receiver-poll-object", -1002))
            val failure = assertFailsWith<BridgeContractException> { withTimeout(BOUND) { receiver.objects.next() } }
            assertEquals(-1002, failure.failure.code)
            assertEquals(null, receiver.inventory.value.terminal)
            backend.expireWait()
            runCatching { endpoint.closeAndJoin() }
        }
}
