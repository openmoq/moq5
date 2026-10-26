package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * What the publisher's live two-track loop has to be written against, measured
 * on the ACCEPTED components rather than assumed.
 *
 * Every row here is an observation of current product behaviour -- the real
 * [BoundedCaptureHandoff] over the real [BoundedCaptureLedger], and the real
 * [PlatformCaptureSession] -- and each one names the loop rule it forces. The
 * loop itself is not implemented here, and nothing below stands in for it.
 *
 * Nothing here opens a camera, a codec or a socket.
 */
class LivePublishLoopRedTest {

    private class Track(val kind: MediaKind, sink: CaptureFaultSink) {
        val buffers = HostBuffers()
        val releases = HostReleases()
        val ledger = BoundedCaptureLedger(kind, GENERATION, capacity = 16)
        val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, sink, CAPACITY, HOST_MAX_OUTPUT_BYTES)

        /** One encoder callback's worth of output, observed the way the platform would. */
        fun output(index: Int, bytes: ByteArray, pts: Long, captureNanos: Long, flags: Int = CaptureContract.FLAG_KEY_FRAME): CodecOutput {
            buffers.put(index, ByteBuffer.wrap(bytes))
            if (flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && bytes.isNotEmpty()) {
                assertEquals(ObserveOutcome.Recorded, ledger.observe(GENERATION, kind, CaptureObservation(pts, captureNanos)))
            }
            return CodecOutput(index, 0, bytes.size, pts, flags)
        }

        fun eos(index: Int): CodecOutput {
            buffers.put(index, ByteBuffer.allocate(0))
            return CodecOutput(index, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM)
        }
    }

    /** Counts the wakes a consumer would receive, per kind, without draining anything. */
    private class Wakes : OutputSignal {
        private val counts = mutableMapOf<MediaKind, Int>()

        @Synchronized
        override fun onOutputs(kind: MediaKind) {
            counts[kind] = (counts[kind] ?: 0) + 1
        }

        @Synchronized
        fun count(kind: MediaKind): Int = counts[kind] ?: 0
    }

    /**
     * An empty live track is NOT a finished one.
     *
     * `BroadcastSession.publish` reads `encoder.next() ?: break`, and
     * `EncodedOutputSource` documents `null` as "none left". Against a live
     * track that rule ends the broadcast before its first frame: `null` here
     * means "nothing right now", and only the drained terminal -- or the
     * close -- ends it.
     */
    @Test
    fun l1_anIdleLiveTrackIsNotItsEnd() {
        val track = Track(MediaKind.Video, HostFaults())
        assertNull(track.handoff.next(), "a live track is empty between callbacks")
        assertTrue(!track.handoff.ended, "and being empty is not its end")

        val bytes = ByteArray(4) { (it + 1).toByte() }
        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.output(0, bytes, 33_333L, 1_000_500_000L)))
        assertNotNull(track.handoff.next(), "the frame that arrived afterwards is drainable")
        assertNull(track.handoff.next(), "and then it is idle again, still not ended")
        assertTrue(!track.handoff.ended)

        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.eos(1)))
        val terminal = assertNotNull(track.handoff.next(), "the terminal is drained like anything else")
        assertTrue(terminal.output.isEndOfStream, "and it is what ends the track")
        assertTrue(track.handoff.ended)
    }

    /**
     * The wake is an EDGE, not a level: `signal` installs the sink and replays
     * nothing, and a queue that is already non-empty raises nothing further.
     *
     * So a loop that checks the queue and registers afterwards can suspend
     * holding outputs it will never be woken for. The registration has to come
     * FIRST, and the queue has to be drained after it.
     */
    @Test
    fun l2_aSignalInstalledAfterTheQueueIsCheckedLosesItsWake() {
        val track = Track(MediaKind.Video, HostFaults())
        val wakes = Wakes()
        val bytes = ByteArray(4) { (it + 1).toByte() }
        // The output arrives BEFORE the consumer registers: nothing was
        // installed to signal, and nothing is replayed to it.
        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.output(0, bytes, 33_333L, 1_000_500_000L)))
        track.handoff.signal(wakes)
        assertEquals(0, wakes.count(MediaKind.Video), "installing the sink replays nothing")

        // A second output does not raise one either: the queue was already
        // non-empty, so there is no empty-to-non-empty edge to signal.
        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.output(1, bytes, 66_666L, 1_001_000_000L)))
        assertEquals(0, wakes.count(MediaKind.Video), "and a non-empty queue raises no further wake")
        assertNotNull(track.handoff.next(), "yet two outputs are sitting there to be drained")
        assertNotNull(track.handoff.next())
    }

    /** One wake can stand for several outputs: coalescing must not cost a payload or a terminal. */
    @Test
    fun l3_oneWakeCoversEveryOutputQueuedBehindIt() {
        val track = Track(MediaKind.Video, HostFaults())
        val wakes = Wakes()
        track.handoff.signal(wakes)
        val first = ByteArray(4) { (0x10 + it).toByte() }
        val second = ByteArray(3) { (0x20 + it).toByte() }
        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.output(0, first, 33_333L, 1_000_500_000L)))
        assertEquals(1, wakes.count(MediaKind.Video), "the empty-to-non-empty edge woke the consumer once")
        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.output(1, second, 66_666L, 1_001_000_000L)))
        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.eos(2)))
        assertEquals(1, wakes.count(MediaKind.Video), "and nothing behind it raised another")

        // The consumer that answers that one wake must drain until the queue is
        // empty, or it strands what was coalesced behind it.
        val drained = generateSequence { track.handoff.next() }.toList()
        assertEquals(3, drained.size, "every output queued behind that one wake")
        assertContentEquals(first, drained[0].output.copied(), "the exact bytes of the first")
        assertContentEquals(second, drained[1].output.copied(), "and of the second")
        assertEquals(0, drained[2].output.size, "the terminal carries no media")
        assertEquals(
            listOf(33_333L, 66_666L, 0L),
            drained.map { it.output.presentationTimeUs },
            "each output's own presentation time, in order",
        )
        assertEquals(
            listOf(1_000_500_000L, 1_001_000_000L),
            drained.take(2).map { it.captureElapsedNanos },
            "and the capture instant each one claimed from the ledger",
        )
        assertTrue(drained.last().output.isEndOfStream, "with the terminal behind them")
        assertTrue(track.handoff.ended)
    }

    /**
     * Every terminal raises the wake WHEN the observers it notifies first
     * return normally. That qualification is the point: see [l6].
     */
    @Test
    fun l4_everyTerminalWakesTheConsumerWhenItsObserversReturn() {
        val ending = Track(MediaKind.Video, HostFaults())
        val onEos = Wakes()
        ending.handoff.signal(onEos)
        assertEquals(HandoffOutcome.Queued, ending.handoff.onOutput(GENERATION, ending.eos(0)))
        assertEquals(1, onEos.count(MediaKind.Video), "the end-of-stream is an output like any other")

        val failing = Track(MediaKind.Audio, HostFaults())
        val onFault = Wakes()
        failing.handoff.signal(onFault)
        failing.handoff.onCodecError(GENERATION, "CodecException: the encoder died")
        assertEquals(1, onFault.count(MediaKind.Audio), "a terminal fault wakes it too")

        val closing = Track(MediaKind.Video, HostFaults())
        val onClose = Wakes()
        closing.handoff.signal(onClose)
        closing.handoff.close(CloseMode.Completed)
        assertEquals(1, onClose.count(MediaKind.Video), "and so does the close")
    }

    /** An owner whose notifications fail, the way application code can. */
    private class RaisingSink(private val raise: () -> Throwable) : CaptureFaultSink {
        override fun onFault(generation: Long, fault: CaptureFault) {
            throw raise()
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            throw raise()
        }
    }

    /**
     * The wake is raised AFTER the owner has been told, and the telling is not
     * guarded: `onCodecError` calls `faults.onFault` before it signals, and
     * `close` delivers the discard witness before it signals. An observer that
     * throws therefore keeps the wake from ever being raised, even though the
     * track has really ended.
     *
     * This is what the current component does, measured -- not a defect claim
     * and not a fix. It is why a live loop cannot treat "a terminal always
     * wakes me" as an invariant, and why its supervision has to have a route
     * that does not depend on this signal.
     */
    @Test
    fun l6_aThrowingObserverKeepsTheTerminalFromWakingTheConsumer() {
        val raised = IllegalStateException("the owner's notification threw")
        val faulting = Track(MediaKind.Video, RaisingSink { raised })
        val wakes = Wakes()
        faulting.handoff.signal(wakes)
        val fromFault = runCatching { faulting.handoff.onCodecError(GENERATION, "CodecException: the encoder died") }
        assertSame(raised, fromFault.exceptionOrNull(), "the observer's failure leaves through the callback")
        assertTrue(faulting.handoff.ended, "the track really has ended")
        assertEquals(0, wakes.count(MediaKind.Video), "and the consumer was never woken for it")

        val discarding = Track(MediaKind.Audio, RaisingSink { raised })
        val onClose = Wakes()
        discarding.handoff.signal(onClose)
        val bytes = ByteArray(4) { (it + 1).toByte() }
        assertEquals(HandoffOutcome.Queued, discarding.handoff.onOutput(GENERATION, discarding.output(0, bytes, 21_333L, 1_000_250_000L)))
        // That queued output raised its own wake; what is measured here is
        // whether the CLOSE behind it raises one.
        val beforeClose = onClose.count(MediaKind.Audio)
        assertEquals(1, beforeClose, "the queued output woke the consumer")
        val fromClose = runCatching { discarding.handoff.close(CloseMode.Failed) }
        assertSame(raised, fromClose.exceptionOrNull(), "the discard witness failure leaves the same way")
        assertTrue(discarding.handoff.ended)
        assertEquals(beforeClose, onClose.count(MediaKind.Audio), "and the close itself raised no further wake")
        assertNull(discarding.handoff.next(), "with the queue discarded, there is nothing to find either")
    }

    private class NoAcquire : CaptureAcquirer {
        override val order: List<CaptureResource> = emptyList()

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? = null
    }

    private class Granted(override val generation: Long) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.Granted

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored("granted")

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Ready
    }

    /** Forwards each track's facts into the session, and counts what passed through. */
    private class Witnessed : CaptureFaultSink {
        var to: CaptureFaultSink? = null
        val discards = mutableListOf<Pair<MediaKind, Int>>()
        val faults = AtomicInteger(0)

        override fun onFault(generation: Long, fault: CaptureFault) {
            faults.incrementAndGet()
            to?.onFault(generation, fault)
        }

        @Synchronized
        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            discards.add(kind to outputs)
            to?.onDiscarded(generation, kind, outputs)
        }

        @Synchronized
        fun witnessed(): List<Pair<MediaKind, Int>> = discards.toList()
    }

    /**
     * The discard witness reaches the session's sink and stops there.
     *
     * The count is real and exact, and `CaptureSessionListener` has no event
     * to carry it, so the owner that decides what a lost frame means is never
     * told. This is the open route, measured rather than asserted.
     */
    @Test
    fun l5_theDiscardWitnessReachesTheSessionAndNoOwner() {
        val witness = Witnessed()
        val video = Track(MediaKind.Video, witness)
        val audio = Track(MediaKind.Audio, witness)
        val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        val listener = HostSessionListener()
        val session = PlatformCaptureSession(GENERATION, ownership, NoAcquire(), video.handoff, audio.handoff, listener)
        witness.to = session
        assertEquals(CaptureStart.Started, session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available))

        val bytes = ByteArray(4) { (it + 1).toByte() }
        assertEquals(HandoffOutcome.Queued, video.handoff.onOutput(GENERATION, video.output(0, bytes, 33_333L, 1_000_500_000L)))
        assertEquals(HandoffOutcome.Queued, video.handoff.onOutput(GENERATION, video.output(1, bytes, 66_666L, 1_001_000_000L)))

        session.stop(IllegalStateException("the user left"))
        assertEquals(listOf(MediaKind.Video to 2), witness.witnessed(), "the track witnessed exactly what it threw away")
        val said = listener.all()
        assertEquals(2, said.size, "and the owner was told two things: $said")
        assertEquals(CaptureReady.Ready, said[0], "the readiness")
        assertIs<CaptureTermination>(said[1], "and the ending, with no count in either")
        assertTrue(
            said.none { it is CaptureReady.Refused },
            "nothing on this listener carries a discarded-output count: there is no event for it",
        )
    }

    /** The declared range of one drained output, as the loop would write it. */
    private fun EncodedOutput.copied(): ByteArray = buffer.copyOfRange(offset, offset + size)

    private companion object {
        const val GENERATION = 1L
        const val CAPACITY = 4
    }
}
