package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendAttachResult
import org.openmoq.moq5.engine.BackendCommandResult
import org.openmoq.moq5.engine.BackendSenderTrackResult
import org.openmoq.moq5.engine.BackendWriteResult
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The public media sender, RED-first over the scripted backend
 * (`media_sender.h`).
 *
 * Derived contract:
 *  * [MoqEndpoint.attachSender] is `moq_media_sender_attach` on the service
 *    worker: one sender per endpoint (a second attach is the C's
 *    `WRONG_STATE`, surfaced exactly), and a sender and a receiver share
 *    one endpoint with independent lifecycles. Endpoint teardown destroys
 *    an attached sender before the endpoint stop, exactly once.
 *  * Every native call is worker-confined and runs only on the caller's
 *    demand. There is no C `moq_media_sender_wait` in this binding: it
 *    parks a thread inside C and consumes the endpoint's single activity
 *    level, which the engine's own loop owns.
 *  * [MediaSender.tryWrite] is ONE `moq_media_sender_write`: [WriteOutcome.Accepted]
 *    means the bytes were transferred; [WriteOutcome.WouldBlock] is a VALUE
 *    (flow control, nothing transferred, retry the same object), never an
 *    exception. The attempt is bounded (the admitted policies never park)
 *    and its outcome is always delivered: a caller cancelled while the
 *    attempt is in flight still receives the outcome, so it always knows
 *    whether transfer happened. Permanent refusals are typed and transfer
 *    nothing.
 *  * [MediaSender.write] is the C retry loop made honest: `tryWrite`, and
 *    on `WouldBlock` suspend until the engine completes its next pass
 *    (queue drain and the readiness flip both happen on pump cycles, which
 *    mark the endpoint's activity), then try again, until accepted or the
 *    caller's timeout, which answers `WouldBlock` with nothing transferred.
 *    No timer, no fixed slice, no C wait. Under a drop policy the C never
 *    blocks on a full queue, so `write` returns on the first attempt; the
 *    loop never turns a drop policy into lossless suspension.
 *  * [MediaSender.endTrack] queues the reliable END_OF_TRACK marker with the
 *    same would-block shape; it is idempotent and ends nothing else.
 *  * [MediaSender.removeTrack] retires the handle: later operations on it
 *    refuse locally, without a native call.
 *  * [MediaSender.convertToVod] is one atomic native call over the batch.
 *  * [MediaSender.complete] RECORDS the completion: accepted is not emitted,
 *    no drain is implied or performed, and the documented barrier is
 *    complete, then [MoqEndpoint.drain], then close.
 *  * [MediaSender.status] is one native probe per pass (ready, terminal,
 *    media demand). The C demand callbacks are installed by the bridge only
 *    to wake the endpoint, so a demand change produces a pass promptly; the
 *    value is always read from the C queries, never composed from callbacks.
 *    A terminal beats readiness. [MediaSender.awaitReady] suspends on it.
 *  * Sender-owned track identity: a [SendTrack] belongs to the sender that
 *    added it; a foreign handle is refused before any native call.
 */
internal class PublicSenderSurfaceTest : EngineFixture() {
    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = SenderConfig(namespace = NAMESPACE, backpressure = SendBackpressure.ReturnWouldBlock)
        val DROPPING = CONFIG.copy(backpressure = SendBackpressure.DropToKeyframe)
        val RX = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe)
        val VIDEO =
            SendTrackConfig(
                name = TrackName("v".toByteArray()), mediaType = MediaType.Video, packaging = PackagingKind.Raw,
                codec = MediaBytes("avc1.64001f".toByteArray()), bitrate = 2_000_000uL,
            )
        const val INVAL = -2
        const val PROTO = -3
        const val CLOSED = -4
        const val WRONG_STATE = -5
        const val INTERRUPTED = -13
        fun key(bytes: String, pt: ULong = 0uL) = SendObject(payload = MediaBytes(bytes.toByteArray()), isSync = true, startsGroup = true, presentationTime = MediaMicros(pt))
        fun delta(bytes: String, pt: ULong = 0uL) = SendObject(payload = MediaBytes(bytes.toByteArray()), isSync = false, startsGroup = false, presentationTime = MediaMicros(pt))
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun ready(backend: ScriptedEndpointBackend, sender: MediaSender) {
        backend.senderReady = true
        backend.markActivity()
        withTimeout(BOUND) { sender.awaitReady() }
    }

    @Test
    fun `attachSender attaches once on the worker with the exact config, a second attach is the C refusal, and a receiver shares the endpoint`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val seen = backend.observed()
            assertEquals(1, seen.senderAttaches.size)
            assertEquals(CONFIG, seen.senderAttaches.single().config)
            assertEquals(seen.serviceThreadId, seen.senderAttaches.single().threadId, "attach runs on the endpoint's worker")
            assertTrue(!sender.isClosed)
            backend.senderAttachResult = BackendAttachResult.Refused(MoqResultCode(WRONG_STATE))
            assertEquals(MoqResultCode(WRONG_STATE), assertFailsWith<SenderAttachFailedException> { endpoint.attachSender(CONFIG) }.code)
            val receiver = endpoint.attachReceiver(RX)
            assertTrue(!receiver.isClosed && !sender.isClosed, "one attachment slot per kind")
            endpoint.closeAndJoin()
            val order = backend.observed().callOrder.filter { it in setOf("receiver-destroy", "sender-destroy", "stop", "destroy") }
            assertEquals(listOf("receiver-destroy", "sender-destroy", "stop", "destroy"), order, "both children are destroyed before the stop the C would refuse")
            assertTrue(sender.isClosed)
        }

    @Test
    fun `a sender closed by the app is destroyed once, frees the slot, and refuses everything afterwards without a native call`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            withTimeout(BOUND) { sender.close() }
            withTimeout(BOUND) { sender.close() }
            assertEquals(1, backend.observed().senderCloses.size, "idempotent: one native destroy")
            val before = backend.observed()
            assertFailsWith<SenderClosedException> { sender.tryWrite(track, key("k")) }
            assertFailsWith<SenderClosedException> { sender.addTrack(VIDEO) }
            assertFailsWith<SenderClosedException> { sender.endTrack(track, Duration.ZERO) }
            assertFailsWith<SenderClosedException> { sender.complete() }
            assertFailsWith<SenderClosedException> { sender.statsSnapshot() }
            assertEquals(before.callOrder, backend.observed().callOrder, "a closed sender makes no native call")
            val again = endpoint.attachSender(CONFIG)
            assertEquals(2, backend.observed().senderAttaches.size, "the slot is free again")
            assertTrue(!again.isClosed)
            endpoint.closeAndJoin()
            assertEquals(2, backend.observed().senderCloses.size, "the first sender is never destroyed twice")
        }

    @Test
    fun `addTrack returns a sender-owned handle, a foreign handle is refused before any native call, and a busy name is the RETRYABLE refusal`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            val added = backend.observed().senderAddTracks.single()
            assertEquals(VIDEO, added.config)
            assertEquals(backend.observed().serviceThreadId, added.threadId)
            assertEquals(VIDEO, track.config)
            assertEquals(VIDEO.name, track.name)
            backend.scriptSenderAddTrack(BackendSenderTrackResult.WouldBlock)
            val busy = assertFailsWith<TrackNameBusyException> { withTimeout(BOUND) { sender.addTrack(VIDEO) } }
            assertEquals(VIDEO.name, busy.name, "the name (or its generated sibling) is still tearing down: RETRY later, it is not a duplicate")
            val again = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            assertEquals(VIDEO.name, again.name, "the same add, retried, is accepted once the teardown is done")
            backend.scriptSenderAddTrack(BackendSenderTrackResult.Refused(MoqResultCode(INVAL)))
            assertEquals(MoqResultCode(INVAL), assertFailsWith<TrackConfigRejectedException> { withTimeout(BOUND) { sender.addTrack(VIDEO) } }.code)
            val otherBackend = newBackend()
            val other = MoqEndpoint.open(EndpointConfig(URL)) { otherBackend }
            val foreignSender = other.attachSender(CONFIG)
            val before = otherBackend.observed().callOrder
            assertFailsWith<ForeignSendTrackException> { foreignSender.tryWrite(track, key("k")) }
            assertFailsWith<ForeignSendTrackException> { foreignSender.endTrack(track, Duration.ZERO) }
            assertFailsWith<ForeignSendTrackException> { foreignSender.removeTrack(track) }
            assertFailsWith<ForeignSendTrackException> { foreignSender.convertToVod(listOf(VodConversion(track, MediaMillis(1uL)))) }
            assertEquals(before, otherBackend.observed().callOrder, "a foreign handle never reaches the bridge")
            other.closeAndJoin()
            endpoint.closeAndJoin()
        }

    @Test
    fun `tryWrite transfers on Accepted only -- WouldBlock is a value that transfers nothing, and the same object is retried`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            backend.scriptSenderWrite(BackendWriteResult.WouldBlock)
            val obj = key("k1", 10uL)
            assertEquals(WriteOutcome.WouldBlock, withTimeout(BOUND) { sender.tryWrite(track, obj) })
            assertEquals(0, backend.observed().acceptedPayloads.size, "nothing transferred on WouldBlock")
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { sender.tryWrite(track, obj) })
            val writes = backend.observed().senderWrites
            assertEquals(2, writes.size)
            assertTrue(writes.all { it.threadId == backend.observed().serviceThreadId }, "every attempt on the worker")
            assertEquals(listOf("k1"), backend.observed().acceptedPayloads.map { String(it) })
            assertEquals(obj, writes[1].value, "the identical object crossed on the retry")
            assertEquals(0, backend.observed().senderWaits, "the binding never calls the C wait")
            endpoint.closeAndJoin()
        }

    @Test
    fun `an in-flight tryWrite delivers its outcome to a caller cancelled mid-attempt -- the caller always learns whether transfer happened`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            backend.holdNextSenderWrite()
            // A cancelled Deferred can never report a value, so the outcome the CALLING coroutine received is proven through a side channel it fills itself.
            val delivered = CompletableDeferred<Result<WriteOutcome>>()
            val attempt = launch(Dispatchers.Default) { delivered.complete(runCatching { sender.tryWrite(track, key("k")) }) }
            assertTrue(backend.awaitCondition("held") { it.senderWriteHeld })
            attempt.cancel()
            backend.releaseSenderWrite()
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { delivered.await() }.getOrThrow(), "the claimed attempt's outcome is delivered, not a cancellation")
            withTimeout(BOUND) { attempt.join() }
            assertEquals(1, backend.observed().senderWrites.size, "one attempt, never a second")
            assertEquals(listOf("k"), backend.observed().acceptedPayloads.map { String(it) })
            endpoint.closeAndJoin()
        }

    @Test
    fun `write retries only on passes -- no timer, no C wait -- and answers WouldBlock at the timeout with nothing transferred`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            backend.scriptSenderWrite(BackendWriteResult.WouldBlock, BackendWriteResult.WouldBlock)
            val writing = async(Dispatchers.Default) { sender.write(track, key("k"), 10.seconds) }
            assertTrue(backend.awaitCondition("first attempt") { it.senderWrites.size == 1 })
            assertTrue(!backend.awaitCondition("no second attempt without a pass", 200) { it.senderWrites.size >= 2 }, "a WouldBlock waits for activity; there is no polling")
            backend.markActivity()
            assertTrue(backend.awaitCondition("second attempt on the pass") { it.senderWrites.size == 2 })
            backend.markActivity()
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { writing.await() })
            assertEquals(3, backend.observed().senderWrites.size, "one attempt per pass, then accepted")
            assertEquals(0, backend.observed().senderWaits)
            backend.scriptSenderWrite(BackendWriteResult.WouldBlock)
            backend.senderWriteDefault = BackendWriteResult.WouldBlock
            assertEquals(WriteOutcome.WouldBlock, withTimeout(BOUND) { sender.write(track, key("late"), 50.milliseconds) })
            assertEquals(listOf("k"), backend.observed().acceptedPayloads.map { String(it) }, "the timed-out object was never transferred")
            endpoint.closeAndJoin()
        }

    @Test
    fun `under a drop policy write is one attempt -- the C never blocks on a full queue, and a GOP the bound cannot hold is WouldBlock at once`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(DROPPING)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            backend.senderWriteDefault = BackendWriteResult.WouldBlock
            assertEquals(WriteOutcome.WouldBlock, withTimeout(BOUND) { sender.write(track, key("huge"), 10.seconds) })
            assertEquals(1, backend.observed().senderWrites.size, "a drop policy's WouldBlock is not flow control: no retry loop")
            endpoint.closeAndJoin()
        }

    @Test
    fun `permanent write refusals are typed with the exact code and transfer nothing`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            backend.scriptSenderWrite(BackendWriteResult.Refused(MoqResultCode(WRONG_STATE)))
            assertFailsWith<SendTrackUnavailableException> { sender.tryWrite(track, key("k")) }
            backend.scriptSenderWrite(BackendWriteResult.Refused(MoqResultCode(INVAL)))
            assertEquals(MoqResultCode(INVAL), assertFailsWith<ObjectRejectedException> { sender.tryWrite(track, key("k")) }.code)
            backend.scriptSenderWrite(BackendWriteResult.Refused(MoqResultCode(PROTO)))
            assertEquals(MoqResultCode(PROTO), assertFailsWith<ObjectRejectedException> { sender.tryWrite(track, key("k")) }.code, "a CMAF validation failure is the C's PROTO, carried exactly")
            backend.scriptSenderWrite(BackendWriteResult.Refused(MoqResultCode(INTERRUPTED)))
            assertFailsWith<SenderInterruptedException> { sender.tryWrite(track, key("k")) }
            backend.senderFatal = 0x3uL
            backend.scriptSenderWrite(BackendWriteResult.Refused(MoqResultCode(CLOSED)))
            val closed = assertFailsWith<SenderClosedException> { sender.tryWrite(track, key("k")) }
            assertEquals(SenderTerminal.Fatal(SenderFatalCode.NAMESPACE_REJECTED), closed.terminal, "CLOSED with the sender's own fatal code")
            assertEquals(0, backend.observed().acceptedPayloads.size)
            assertTrue(!sender.isClosed, "a terminal is not the app's close: the handle stays until close()")
            endpoint.closeAndJoin()
        }

    @Test
    fun `endTrack queues the reliable marker with the write's would-block shape, is idempotent, and ends only that track`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val a = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            val b = withTimeout(BOUND) { sender.addTrack(VIDEO.copy(name = TrackName("w".toByteArray()))) }
            ready(backend, sender)
            backend.scriptSenderEndTrack(BackendWriteResult.WouldBlock)
            assertEquals(WriteOutcome.WouldBlock, withTimeout(BOUND) { sender.tryEndTrack(a) })
            val ending = async(Dispatchers.Default) { sender.endTrack(a, 10.seconds) }
            assertTrue(backend.awaitCondition("attempt") { it.senderEndTracks.size == 2 })
            backend.markActivity()
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { ending.await() })
            assertTrue(a.isEnded, "an ACCEPTED end marker is remembered locally")
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { sender.endTrack(a, Duration.ZERO) }, "idempotent: the C answers OK again")
            assertEquals(3, backend.observed().senderEndTracks.size, "a repeated endTrack still crosses: the C's idempotence contract is kept, not simulated")
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { sender.tryWrite(b, key("still open")) }, "ending a does not end b")
            val writes = backend.observed().senderWrites.size
            assertFailsWith<SendTrackUnavailableException> { sender.tryWrite(a, key("after end")) }
            assertFailsWith<SendTrackUnavailableException> { sender.write(a, key("after end"), Duration.ZERO) }
            assertEquals(writes, backend.observed().senderWrites.size, "a write after an accepted end refuses BEFORE the bridge: the C would answer WRONG_STATE")
            assertTrue(!b.isEnded)
            endpoint.closeAndJoin()
        }

    @Test
    fun `removeTrack retires the handle locally -- later operations refuse without a native call -- and the C refusals cross exactly`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            withTimeout(BOUND) { sender.removeTrack(track) }
            assertEquals(listOf(track.token), backend.observed().senderRemoves.map { it.track })
            assertTrue(track.isRemoved)
            val before = backend.observed().callOrder
            assertFailsWith<SendTrackUnavailableException> { sender.tryWrite(track, key("k")) }
            assertFailsWith<SendTrackUnavailableException> { sender.removeTrack(track) }
            assertFailsWith<SendTrackUnavailableException> { sender.endTrack(track, Duration.ZERO) }
            assertEquals(before, backend.observed().callOrder, "a retired handle never reaches the bridge")
            val other = withTimeout(BOUND) { sender.addTrack(VIDEO.copy(name = TrackName("w".toByteArray()))) }
            backend.senderCommandResult = BackendCommandResult.Refused(MoqResultCode(INTERRUPTED))
            assertFailsWith<SenderInterruptedException> { sender.removeTrack(other) }
            assertTrue(!other.isRemoved, "a refused removal retires nothing")
            endpoint.closeAndJoin()
        }

    @Test
    fun `convertToVod is one atomic native call over the batch, refused as a whole with the exact code`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val a = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            val b = withTimeout(BOUND) { sender.addTrack(VIDEO.copy(name = TrackName("w".toByteArray()))) }
            ready(backend, sender)
            withTimeout(BOUND) { sender.convertToVod(listOf(VodConversion(a, MediaMillis(90_000uL)), VodConversion(b, MediaMillis(60_000uL)))) }
            val call = backend.observed().senderVodConversions.single()
            assertEquals(listOf(a.token to 90_000uL, b.token to 60_000uL), call.items, "both items in ONE call, in order")
            assertEquals(backend.observed().serviceThreadId, call.threadId)
            assertTrue(a.isVod && b.isVod, "an accepted conversion is remembered on every item")
            val writes = backend.observed().senderWrites.size
            assertFailsWith<SendTrackUnavailableException> { sender.tryWrite(a, key("no new content")) }
            assertEquals(writes, backend.observed().senderWrites.size, "a write to a VOD track refuses before the bridge: the C would answer WRONG_STATE")
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { sender.tryEndTrack(a) }, "end_track stays available after conversion")
            withTimeout(BOUND) { sender.removeTrack(b) }
            assertTrue(b.isRemoved, "remove_track stays available after conversion")
            val c = withTimeout(BOUND) { sender.addTrack(VIDEO.copy(name = TrackName("x".toByteArray()))) }
            val d = withTimeout(BOUND) { sender.addTrack(VIDEO.copy(name = TrackName("y".toByteArray()))) }
            backend.senderCommandResult = BackendCommandResult.Refused(MoqResultCode(INVAL))
            assertEquals(MoqResultCode(INVAL), assertFailsWith<VodConversionRejectedException> { sender.convertToVod(listOf(VodConversion(c, MediaMillis(1uL)), VodConversion(d, MediaMillis(2uL)))) }.code)
            assertTrue(!c.isVod && !d.isVod, "a refused batch converts NOTHING: no item flips")
            backend.senderCommandResult = null
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { sender.tryWrite(c, key("still live")) })
            assertFailsWith<IllegalArgumentException> { sender.convertToVod(emptyList()) }
            endpoint.closeAndJoin()
        }

    @Test
    fun `complete is accepted, not emitted -- no drain is performed or implied, and later mutations are the C WRONG_STATE, typed`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            backend.senderCommandResult = BackendCommandResult.Refused(MoqResultCode(WRONG_STATE))
            assertFailsWith<SenderNotReadyException> { sender.complete() }
            backend.senderCommandResult = null
            ready(backend, sender)
            withTimeout(BOUND) { sender.complete() }
            withTimeout(BOUND) { sender.complete() }
            assertEquals(3, backend.observed().senderCompletes.size, "every call crosses -- the refused pre-ready one and the two idempotent OKs")
            assertEquals(0, backend.observed().drainCalls, "complete never drains: the barrier is the app's endpoint.drain")
            backend.scriptSenderWrite(BackendWriteResult.Refused(MoqResultCode(WRONG_STATE)))
            assertFailsWith<SendTrackUnavailableException> { sender.tryWrite(track, key("k")) }
            backend.scriptSenderAddTrack(BackendSenderTrackResult.Refused(MoqResultCode(WRONG_STATE)))
            assertFailsWith<SenderCompletedException> { sender.addTrack(VIDEO.copy(name = TrackName("w".toByteArray()))) }
            assertTrue(!sender.isClosed, "completion is not close")
            endpoint.closeAndJoin()
        }

    @Test
    fun `status is one native probe per pass -- ready, media demand, then a fatal with its own code -- and a terminal beats readiness`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            assertEquals(SenderStatus(ready = false, terminal = null, mediaDemand = false), sender.status.value)
            val probesAtRest = backend.observed().senderProbes
            backend.senderReady = true
            backend.markActivity()
            assertEquals(true, withTimeout(BOUND) { sender.status.first { it.ready } }.ready)
            assertEquals(probesAtRest + 1, backend.observed().senderProbes, "one probe on the pass, none in between")
            backend.senderMediaDemand = true
            backend.trackDemand(track.token, 2u)
            backend.markActivity()
            assertTrue(withTimeout(BOUND) { sender.status.first { it.mediaDemand } }.mediaDemand)
            assertEquals(2u, withTimeout(BOUND) { sender.trackDemand(track) }, "the exact native count")
            backend.senderFatal = 0x5uL
            backend.markActivity()
            val fatal = withTimeout(BOUND) { sender.status.first { it.terminal != null } }
            assertEquals(SenderTerminal.Fatal(SenderFatalCode.PUBLISH_REJECTED), fatal.terminal)
            assertEquals(false, fatal.ready, "is_ready is ready AND not fatal")
            assertFailsWith<SenderClosedException> { withTimeout(BOUND) { sender.awaitReady() } }
            assertTrue(!sender.isClosed)
            endpoint.closeAndJoin()
        }

    @Test
    fun `awaitReady resumes on the ready pass, and an endpoint close underneath is the Closed terminal, not a fatal`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val waiting = async(Dispatchers.Default) { sender.awaitReady() }
            assertTrue(!backend.awaitCondition("not ready", 200) { false })
            assertTrue(waiting.isActive, "parked until a pass reports readiness")
            backend.senderReady = true
            backend.markActivity()
            withTimeout(BOUND) { waiting.await() }
            backend.senderEndpointClosed = true
            backend.markActivity()
            assertEquals(SenderTerminal.Closed, withTimeout(BOUND) { sender.status.first { it.terminal != null } }.terminal)
            assertEquals(SenderTerminal.Closed, assertFailsWith<SenderClosedException> { sender.awaitReady() }.terminal)
            endpoint.closeAndJoin()
        }

    @Test
    fun `statsSnapshot is one native read on the worker, on demand, with the sender's own fields`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            backend.markActivity()
            assertEquals(0, backend.observed().senderStatsReads.size, "no demand, no read")
            backend.scriptedSenderStats = SenderStats(0x8000_0000_0000_0001uL, 2uL, 3uL, ULong.MAX_VALUE, 5uL, 6uL, 7uL, 8uL, 9uL, MoqResultCode(-8), 11uL)
            val s = withTimeout(BOUND) { sender.statsSnapshot() }
            assertEquals(backend.scriptedSenderStats, s)
            assertEquals(backend.observed().serviceThreadId, backend.observed().senderStatsReads.single().threadId)
            assertEquals(0x8000_0000_0000_0001uL, s.objectsWritten)
            assertEquals(MoqResultCode(-8), s.lastError, "the last non-OK write, as the C result it was")
            endpoint.closeAndJoin()
        }

    @Test
    fun `object validation is the C's -- a property block on a RAW track crosses and is answered as the C's INVAL, transferring nothing`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            val withProps = key("k").copy(cmafProperties = MediaBytes(byteArrayOf(1)))
            backend.scriptSenderWrite(BackendWriteResult.Refused(MoqResultCode(INVAL)))
            assertEquals(MoqResultCode(INVAL), assertFailsWith<ObjectRejectedException> { sender.tryWrite(track, withProps) }.code)
            assertEquals(withProps, backend.observed().senderWrites.single().value, "the object crossed as given: the binding does not reimplement the C's packaging rules")
            assertEquals(0, backend.observed().acceptedPayloads.size)
            assertEquals(WriteOutcome.Accepted, withTimeout(BOUND) { sender.tryWrite(track, delta("d")) }, "anchor policy is the C's, never second-guessed here")
            endpoint.closeAndJoin()
        }

    @Test
    fun `an interrupt latch on the endpoint refuses writes as SenderInterruptedException and the write loop stops at once`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val sender = endpoint.attachSender(CONFIG)
            val track = withTimeout(BOUND) { sender.addTrack(VIDEO) }
            ready(backend, sender)
            backend.senderWriteDefault = BackendWriteResult.Refused(MoqResultCode(INTERRUPTED))
            assertFailsWith<SenderInterruptedException> { withTimeout(BOUND) { sender.write(track, key("k"), 10.seconds) } }
            assertEquals(1, backend.observed().senderWrites.size, "the latch is not flow control: no retry")
            assertNull(sender.status.value.terminal, "the latch is non-terminal")
            endpoint.closeAndJoin()
        }
}
