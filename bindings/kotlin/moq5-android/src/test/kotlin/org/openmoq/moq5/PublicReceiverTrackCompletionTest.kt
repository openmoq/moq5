package org.openmoq.moq5

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.openmoq.moq5.engine.BackendObject
import org.openmoq.moq5.engine.BackendObjectResult
import org.openmoq.moq5.engine.BackendTrackEvent
import org.openmoq.moq5.engine.BackendTrackEventKind
import org.openmoq.moq5.engine.EngineFixture
import org.openmoq.moq5.engine.ScriptedEndpointBackend
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * What the PUBLIC contract does and does not say about a single track being
 * finished, over the scripted backend.
 *
 * This exists because a consumer (the player sample) wants to stop pulling when
 * the track it selected is complete, and 1730/1742 forbid inventing that from
 * an end event, an empty queue or a quiet period. The rows below are the
 * existing-contract half of that question.
 *
 * WHAT THESE PROVE, and only this:
 *  * a per-track ENDED does not end the object stream, and an object queued
 *    behind that ENDED is still delivered to the consumer afterwards -- so
 *    closing on the event would discard it;
 *  * a track REMOVED by catalog change is likewise not a stream terminal, and
 *    what the native layer still answers after a removal is what the consumer
 *    receives -- the public stream neither purges nor invents;
 *  * the stream's own end remains receiver-wide: `null` arrives only with the
 *    receiver's terminal, after the queue drained.
 *
 * WHAT THEY CANNOT PROVE: what the C receiver ADMITS after it marks a track
 * ended. The scripted backend answers what it is told to answer, so an ordering
 * scripted here is evidence about the Kotlin contract, never about wire or
 * intake reachability. That question belongs to `service/tests`, and no claim
 * about a late lower-layer tail is made from these rows.
 *
 * The complementary rows already in this suite are not repeated here:
 * event-first drain (`PublicReceiverObjectTest`, the ADDED-then-object rows),
 * the cancelled-claim staged handoff (same file), the terminal-after-queued
 * rows, and the per-queue exhaustion matrix in
 * `PublicReceiverQueueExhaustionTest`.
 */
internal class PublicReceiverTrackCompletionTest : EngineFixture() {

    private companion object {
        const val URL = "moqt://moq.example:4433/p"
        val BOUND = 5.seconds
        val NAMESPACE = Namespace(listOf(NamespacePart("live".toByteArray())))
        val CONFIG = ReceiverConfig(namespace = NAMESPACE, overflow = OverflowPolicy.DropToKeyframe)
        val T1 = -0x7fff_ffff_ffff_fefdL
        val VIDEO =
            TrackDescription(
                name = MediaBytes("a".toByteArray()),
                packaging = MediaBytes("loc".toByteArray()),
                role = MediaBytes("video".toByteArray()),
            )

        fun added(t: Long, d: TrackDescription) = BackendTrackEvent(BackendTrackEventKind.Added, t, d)

        fun ended(t: Long) = BackendTrackEvent(BackendTrackEventKind.Ended, t, null)

        fun removed(t: Long) = BackendTrackEvent(BackendTrackEventKind.Removed, t, null)

        val ready = BackendTrackEvent(BackendTrackEventKind.CatalogReady, 0L, null)

        fun raw(t: Long, bytes: String) =
            BackendObject(
                track = t,
                payload = MediaPayload.Raw(MediaBytes(bytes.toByteArray())),
                status = ObjectStatus.Normal,
                endOfGroup = false,
                viaDatagram = false,
                keyframe = false,
                captureTime = null,
                decodeTime = MediaMicros(1_000uL),
                compositionOffset = MediaOffsetMicros(0L),
                presentationTime = MediaMicros(1_000uL),
            )
    }

    private fun runTest(body: suspend CoroutineScope.() -> Unit) = runBlocking { body() }

    private suspend fun discovered(backend: ScriptedEndpointBackend, receiver: MediaReceiver): MediaTrack {
        backend.scriptTrackEvents(added(T1, VIDEO), ready)
        backend.markActivity()
        return withTimeout(BOUND) { receiver.inventory.first { it.catalogReady } }.tracks[0]
    }

    @Test
    fun `an object queued behind a per-track ENDED is still delivered, so the end event is not completion`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            val track = discovered(backend, receiver)

            // The end arrives, and one more object for that same track sits
            // behind it in the native queue.
            backend.scriptTrackEvents(ended(T1))
            backend.scriptObjects(raw(T1, "after-the-end"))
            backend.markActivity()

            val ended = withTimeout(BOUND) { receiver.inventory.first { it.tracks[0].presence == TrackPresence.Ended } }
            assertEquals(TrackPresence.Ended, ended.tracks[0].presence, "the track is ended in the inventory")
            assertEquals(null, ended.terminal, "a per-track end is not a receiver terminal")

            val late = withTimeout(BOUND) { receiver.objects.next() }
            assertTrue(late != null, "the object queued behind the ENDED must still be delivered")
            assertEquals("after-the-end", String(late!!.mediaBytes().bytes()))
            assertTrue(late.track === track, "and it names the same discovered track")

            // Closing on the end event would have discarded exactly that object.
            backend.pollObjectTerminal = BackendObjectResult.Closed(fatal = false, code = 0uL)
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "the stream ends with the RECEIVER")
            endpoint.closeAndJoin()
        }

    @Test
    fun `a REMOVED track is not a stream terminal either, and the stream delivers what the native layer answers`() =
        runTest {
            val backend = newBackend()
            val endpoint = MoqEndpoint.open(EndpointConfig(URL)) { backend }
            val receiver = endpoint.attachReceiver(CONFIG)
            discovered(backend, receiver)

            backend.scriptTrackEvents(removed(T1))
            backend.markActivity()
            val gone = withTimeout(BOUND) { receiver.inventory.first { it.tracks[0].presence == TrackPresence.Removed } }
            assertEquals(TrackPresence.Removed, gone.tracks[0].presence)
            assertEquals(null, gone.terminal, "a removal is not a receiver terminal")

            // Whether anything remains for a removed track is the native
            // layer's decision -- ordinary catalog removal disables and purges
            // there. The public stream neither purges nor invents: it answers
            // what the native poll answers, which here is nothing more.
            backend.pollObjectTerminal = BackendObjectResult.Closed(fatal = false, code = 0uL)
            assertEquals(null, withTimeout(BOUND) { receiver.objects.next() }, "no object is invented after a removal")
            endpoint.closeAndJoin()
        }
}
