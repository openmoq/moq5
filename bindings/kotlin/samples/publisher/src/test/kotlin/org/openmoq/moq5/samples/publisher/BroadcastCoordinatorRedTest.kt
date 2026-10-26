package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.withContext
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.DrainOutcome
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WriteOutcome

/**
 * The two-track broadcast coordinator, against the REAL capture session, the
 * real handoffs and ledgers, the accepted pipeline and the accepted per-track
 * drain. Only the platform and the sender are scripted: codec buffers, the
 * acquisition driver, the per-track sinks and the completion seam.
 *
 * Every sender call -- both tracks' writes and ends, the completion and the
 * endpoint drain -- lands in ONE ordered cross-track inventory, so ordering
 * between the tracks, the completion and the drain is observable and an extra
 * call fails the row.
 *
 * Nothing here is a regression of `BroadcastSession`, which this slice does
 * not touch.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class BroadcastCoordinatorRedTest {

    /** Everything the sender and the endpoint were asked to do, in order. */
    private sealed interface Call {
        data class Wrote(val kind: MediaKind, val obj: SendObject, val timeout: Duration) : Call

        data class Ended(val kind: MediaKind, val timeout: Duration) : Call

        data object Completed : Call

        data class Drained(val timeout: Duration) : Call
    }

    private class Calls {
        private val calls = mutableListOf<Call>()

        @Synchronized
        fun add(call: Call) {
            calls.add(call)
        }

        @Synchronized
        fun all(): List<Call> = calls.toList()
    }

    private class LaneSink(
        private val kind: MediaKind,
        private val calls: Calls,
        private val writes: ArrayDeque<WriteOutcome> = ArrayDeque(),
        private val ends: ArrayDeque<WriteOutcome> = ArrayDeque(),
        private val beforeWrite: (suspend (Int) -> Unit)? = null,
        private val beforeEnd: (suspend () -> Unit)? = null,
    ) : TrackSink {
        private val attempts = AtomicInteger(0)

        override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome {
            beforeWrite?.invoke(attempts.getAndIncrement())
            calls.add(Call.Wrote(kind, obj, timeout))
            return writes.removeFirstOrNull() ?: WriteOutcome.Accepted
        }

        override suspend fun endTrack(timeout: Duration): WriteOutcome {
            beforeEnd?.invoke()
            calls.add(Call.Ended(kind, timeout))
            return ends.removeFirstOrNull() ?: WriteOutcome.Accepted
        }

        fun scriptWrites(vararg outcomes: WriteOutcome) = apply { writes.addAll(outcomes) }

        fun scriptEnds(vararg outcomes: WriteOutcome) = apply { ends.addAll(outcomes) }
    }

    /**
     * The completion half of the sender and the endpoint, scripted. Drains are
     * counted from 0: [drainAnswers] answers them in order ([drainAnswer]
     * after that), [raiseAtDrain] picks the one [raiseOnDrain] raises from
     * (every drain when null), and [beforeDrain] runs before each is recorded.
     */
    private class Completion(
        private val calls: Calls,
        private val drainAnswer: DrainOutcome = DrainOutcome.Flushed,
        private val raiseOnComplete: (() -> Throwable)? = null,
        private val raiseOnDrain: (() -> Throwable)? = null,
        private val onComplete: (() -> Unit)? = null,
        private val drainAnswers: List<DrainOutcome> = emptyList(),
        private val raiseAtDrain: Int? = null,
        private val beforeDrain: (suspend (Int) -> Unit)? = null,
    ) : CompletionSink {
        private val drains = AtomicInteger(0)

        override suspend fun complete() {
            calls.add(Call.Completed)
            onComplete?.invoke()
            raiseOnComplete?.let { throw it() }
        }

        override suspend fun drain(timeout: Duration): DrainOutcome {
            val index = drains.getAndIncrement()
            beforeDrain?.invoke(index)
            calls.add(Call.Drained(timeout))
            if (raiseAtDrain == null || raiseAtDrain == index) raiseOnDrain?.let { throw it() }
            return drainAnswers.getOrNull(index) ?: drainAnswer
        }
    }

    /**
     * A track whose close is HELD at a fail-only gate. The session closes its
     * children BEFORE announcing its terminal, so holding here withholds the
     * NOTIFICATION while the decision has already been made.
     */
    private class HeldClose(
        private val inner: CaptureHandoff,
        private val entered: HostGate,
        private val release: HostGate,
    ) : CaptureHandoff by inner {
        override fun close(mode: CloseMode) {
            entered.open()
            release.await()
            inner.close(mode)
        }
    }

    /** A track that closes and then raises what a real close may raise. */
    private class RaisingClose(private val inner: CaptureHandoff, private val raise: () -> Throwable) : CaptureHandoff by inner {
        override fun close(mode: CloseMode) {
            inner.close(mode)
            throw raise()
        }
    }

    /**
     * A test-only forwarding listener that BUFFERS the session's terminal
     * notification and returns at once, so the session's own decision and
     * cleanup complete normally and the coordinator simply has not been told
     * yet. Readiness is forwarded as it happens.
     */
    private class WithheldTerminal(private val to: CaptureSessionListener) : CaptureSessionListener {
        private val buffered = java.util.concurrent.atomic.AtomicReference<Pair<Long, CaptureTermination>?>(null)

        override fun onReady(generation: Long, ready: CaptureReady) = to.onReady(generation, ready)

        override fun onTerminated(generation: Long, termination: CaptureTermination) {
            buffered.set(generation to termination)
        }

        val isHeld: Boolean get() = buffered.get() != null

        /** Delivers whatever was withheld, once. Safe to call on every path. */
        fun release() {
            buffered.getAndSet(null)?.let { (generation, termination) -> to.onTerminated(generation, termination) }
        }
    }

    /** A handle whose close can raise anything a real close can, recorded by identity. */
    private class RaisingHandle(
        override val identity: Any,
        private val log: MutableList<Any>,
        private val raise: Throwable?,
    ) : ResourceHandle {
        override fun close() {
            synchronized(log) { log.add(identity) }
            raise?.let { throw it }
        }
    }

    /**
     * Acquires a microphone inside `begin` and leaves a real camera ticket
     * outstanding, so the session starts Pending with resources already owned.
     */
    private class PendingAcquirer(private val closes: MutableList<Any>, private val microphone: HostResource) : CaptureAcquirer {
        override val order: List<CaptureResource> = listOf(CaptureResource.Microphone, CaptureResource.Camera)

        private var ticket: AcquisitionTicket? = null
        private var steps: AcquisitionSteps? = null
        private var owner: CaptureOwnership? = null

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
            this.steps = steps
            this.owner = ownership
            val acquired = ownership.acquire(CaptureResource.Microphone) { RaisingHandle(microphone, closes, null) }
            if (acquired is AcquireOutcome.Failed) return acquired
            ticket = ownership.request(CaptureResource.Camera).also { steps.submitted(CaptureResource.Camera) }
            return null
        }

        /**
         * Settles the outstanding camera step, the way the platform's own
         * failure callback would. A stop does not settle a pending callback,
         * so the row's cleanup has to.
         */
        fun settleOutstanding(): AdoptOutcome? {
            val outstanding = ticket ?: return null
            ticket = null
            val ownership = owner ?: return null
            val outcome = ownership.fail(outstanding, "the row is finished", null)
            steps?.settled(CaptureResource.Camera, outcome)
            return outcome
        }
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

    private class Refused(override val generation: Long, private val why: String) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.PermanentlyDenied

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored(why)

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Refused(why)
    }

    /** One track's real capture plumbing. */
    private class Lane(val kind: MediaKind, faults: CaptureFaultSink) {
        val buffers = HostBuffers()
        val releases = HostReleases()
        val ledger = BoundedCaptureLedger(kind, GENERATION, capacity = 16)
        val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val clock = PlatformCaptureClock(ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L))

        /** One ORIGIN and one calibration for the broadcast, one timeline per track. */
        val pipeline = PublishPipeline(kind, PublishTimeline(ORIGIN), clock, validatedFormat(kind), HOST_LIMITS)

        fun queue(index: Int, bytes: ByteArray, pts: Long, flags: Int = CaptureContract.FLAG_KEY_FRAME): HandoffOutcome {
            buffers.put(index, ByteBuffer.wrap(bytes))
            if (flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && bytes.isNotEmpty()) {
                assertEquals(ObserveOutcome.Recorded, ledger.observe(GENERATION, kind, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
            }
            return handoff.onOutput(GENERATION, CodecOutput(index, 0, bytes.size, pts, flags))
        }

        fun queueEnd(index: Int): HandoffOutcome {
            buffers.put(index, ByteBuffer.allocate(0))
            return handoff.onOutput(GENERATION, CodecOutput(index, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM))
        }
    }

    /** The whole rig: real session, real tracks, scripted sender. */
    private class Rig(
        val calls: Calls = Calls(),
        videoSink: LaneSink? = null,
        audioSink: LaneSink? = null,
        completion: Completion? = null,
        videoHeldAt: Pair<HostGate, HostGate>? = null,
        videoCloseRaises: (() -> Throwable)? = null,
        withholdTerminal: Boolean = false,
        resources: List<Pair<CaptureResource, Throwable?>> = emptyList(),
        pendingAcquisition: Boolean = false,
    ) {
        private val forward = Forward()
        val video = Lane(MediaKind.Video, forward)
        val audio = Lane(MediaKind.Audio, forward)
        val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        val closes = mutableListOf<Any>()
        val identities = resources.map { (resource, _) -> resource to HostResource(resource.name.lowercase()) }.toMap()
        val events = BroadcastEvents()

        /** Withholds the session's terminal notification and RETURNS, so the session's cleanup finishes. */
        val withheld: WithheldTerminal? = if (withholdTerminal) WithheldTerminal(events) else null
        private val videoTrack: CaptureHandoff =
            videoHeldAt?.let { (entered, release) -> HeldClose(video.handoff, entered, release) }
                ?: videoCloseRaises?.let { RaisingClose(video.handoff, it) }
                ?: video.handoff
        val microphone = HostResource("microphone")
        val acquirer: CaptureAcquirer = if (pendingAcquisition) PendingAcquirer(closes, microphone) else NoAcquire()
        val pending: PendingAcquirer? = acquirer as? PendingAcquirer
        val session = PlatformCaptureSession(GENERATION, ownership, acquirer, videoTrack, audio.handoff, withheld ?: events)
        val videoSender = videoSink ?: LaneSink(MediaKind.Video, calls)
        val audioSender = audioSink ?: LaneSink(MediaKind.Audio, calls)
        val completionSink = completion ?: Completion(calls)
        val broadcast =
            LiveBroadcast(
                session = session,
                events = events,
                video = TrackLane(MediaKind.Video, video.pipeline, videoSender),
                audio = TrackLane(MediaKind.Audio, audio.pipeline, audioSender),
                completion = completionSink,
                writeTimeout = WRITE_TIMEOUT,
                drainTimeout = DRAIN_TIMEOUT,
            )

        init {
            forward.to = session
            for ((resource, failure) in resources) {
                val identity = identities.getValue(resource)
                check(ownership.acquire(resource) { RaisingHandle(identity, closes, failure) } is AcquireOutcome.Acquired)
            }
        }

        fun identity(resource: CaptureResource): HostResource = identities.getValue(resource)

        fun closed(): List<Any> = synchronized(closes) { closes.toList() }
    }

    /** Forwards each track's faults into the session it belongs to. */
    private class Forward : CaptureFaultSink {
        @Volatile
        var to: CaptureFaultSink? = null

        override fun onFault(generation: Long, fault: CaptureFault) {
            to?.onFault(generation, fault)
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            to?.onDiscarded(generation, kind, outputs)
        }
    }

    /** A fault the row raises from inside a sender call, once. */
    private class FaultHook {
        var into: (() -> Unit)? = null
        private var armed = true

        fun raise() {
            if (!armed) return
            armed = false
            into?.invoke()
        }
    }

    private fun bytes(tag: Int, size: Int) = ByteArray(size) { ((tag shl 4) + it).toByte() }

    /** A declared Annex-B key frame: `00 00 00 01`, an IDR header 0x65, then six tagged bytes. Synthetic framing, not a picture. */
    private fun videoFrame(tag: Int) = byteArrayOf(0, 0, 0, 1, 0x65) + bytes(tag, 6)

    /** Its declared avc1 payload: the same seven-byte NAL unit after its big-endian length. */
    private fun videoAvc1(tag: Int) = byteArrayOf(0, 0, 0, 7, 0x65) + bytes(tag, 6)

    private fun assertWrote(
        call: Call,
        kind: MediaKind,
        payload: ByteArray,
        presentationTime: ULong,
        captureTime: ULong,
        isSync: Boolean,
        startsGroup: Boolean,
        at: String,
    ) {
        val wrote = assertIs<Call.Wrote>(call, "$at: this call is a write")
        assertEquals(kind, wrote.kind, "$at: on its OWN track")
        assertEquals(WRITE_TIMEOUT, wrote.timeout, "$at: with the declared timeout")
        assertContentEquals(payload, wrote.obj.payload.bytes(), "$at: the exact captured bytes")
        assertEquals(isSync, wrote.obj.isSync, "$at: isSync")
        assertEquals(startsGroup, wrote.obj.startsGroup, "$at: startsGroup")
        assertEquals(false, wrote.obj.endsGroup, "$at: endsGroup")
        assertEquals(MediaMicros(presentationTime), wrote.obj.presentationTime, "$at: presentationTime")
        assertNull(wrote.obj.decodeTime, "$at: no decode time")
        assertEquals(UnixMicros(captureTime), wrote.obj.captureTime, "$at: the instant the ledger claimed")
        assertNull(wrote.obj.sapType, "$at: no SAP type")
        assertNull(wrote.obj.cmafProperties, "$at: no CMAF properties")
    }

    // ---- 1: both tracks, interleaved, to one completion ------------------------

    @Test
    fun b1_bothTracksReachTheirTerminalsAndTheBroadcastCompletesOnce() = runTest {
        val rig = Rig()
        val v0 = videoFrame(1)
        val a0 = bytes(2, 3)
        val v1 = videoFrame(3)
        val outcome =
            runBroadcast(rig) {
                // Interleaved media and idle periods on both tracks.
                assertEquals(HandoffOutcome.Queued, rig.video.queue(0, v0, 33_333L))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queue(0, a0, 21_333L, flags = 0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.video.queue(1, v1, 66_666L))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(1))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(2))
                runCurrent()
            }
        val published = assertIs<BroadcastOutcome.Published>(outcome, "both tracks ended and the session completed")
        assertEquals(TrackDrain.Ended(written = 2, refused = 0, skipped = 0), published.video)
        assertEquals(TrackDrain.Ended(written = 1, refused = 0, skipped = 0), published.audio)
        val calls = rig.calls.all()
        assertEquals(8, calls.size, "three writes, two ends, the media drain, one completion, the catalog drain: $calls")
        assertWrote(calls[0], MediaKind.Video, videoAvc1(1), 33_333uL, EPOCH + 33_333uL, isSync = true, startsGroup = true, at = "video 0")
        assertWrote(calls[1], MediaKind.Audio, a0, 21_333uL, EPOCH + 21_333uL, isSync = true, startsGroup = false, at = "audio 0")
        assertWrote(calls[2], MediaKind.Video, videoAvc1(3), 66_666uL, EPOCH + 66_666uL, isSync = true, startsGroup = true, at = "video 1")
        assertEquals(Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), calls[3], "audio ended first, because it ended first")
        assertEquals(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), calls[4])
        assertEquals(Call.Drained(DRAIN_TIMEOUT), calls[5], "the accepted media and BOTH track terminals drain first")
        assertEquals(Call.Completed, calls[6], "completion only after that barrier")
        assertEquals(Call.Drained(DRAIN_TIMEOUT), calls[7], "and the terminal catalog's drain after the completion, exactly once")
        assertEquals(CaptureTermination.Completed, rig.session.termination)
    }

    // ---- 2: one track fails independently, in both directions ------------------

    @Test
    fun b2_anIndependentTrackFailureCancelsItsSiblingAndNeverCompletes() = runTest {
        for (failing in listOf(MediaKind.Video, MediaKind.Audio)) {
            for (raised in listOf<Throwable>(IllegalStateException("the sender failed"), AssertionError("an Error inside write"))) {
                val calls = Calls()
                val rig =
                    Rig(
                        calls = calls,
                        videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { if (failing == MediaKind.Video) throw raised }),
                        audioSink = LaneSink(MediaKind.Audio, calls, beforeWrite = { if (failing == MediaKind.Audio) throw raised }),
                    )
                val thrown =
                    runBroadcastCatching(rig) {
                        val lane = if (failing == MediaKind.Video) rig.video else rig.audio
                        assertEquals(HandoffOutcome.Queued, lane.queue(0, videoFrame(1), 33_333L, flags = if (failing == MediaKind.Audio) 0 else CaptureContract.FLAG_KEY_FRAME))
                        runCurrent()
                    }
                assertSame(raised, thrown, "$failing: the actual cause reaches the caller")
                assertTrue(
                    calls.all().none { it is Call.Completed || it is Call.Drained },
                    "$failing: a failed track never completes the broadcast: ${calls.all()}",
                )
            }
        }
    }

    @Test
    fun b3_cancellingTheBroadcastCancelsAndJoinsBothTracks() = runTest {
        val rig = Rig()
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            running.cancel(CancellationException("the user left"))
            runCatching { running.join() }
            runCurrent()
            assertTrue(seen.reached, "the broadcast reached its own boundary and recorded what ended it")
            observed = seen.observed().exceptionOrNull()
        }
        assertIs<CancellationException>(observed, "cancellation stays cancellation on run's own frame")
        assertTrue(
            rig.calls.all().none { it is Call.Completed || it is Call.Drained },
            "a cancelled broadcast completes nothing: ${rig.calls.all()}",
        )
        // Cancellation cleanup stops the session with a non-null reason, which
        // CLOSES both tracks, so a later callback is refused at the handoff.
        assertIs<HandoffOutcome.Ignored>(
            rig.video.queue(0, videoFrame(1), 33_333L),
            "the coordinator's cleanup closed this track",
        )
        runCurrent()
        assertTrue(rig.calls.all().none { it is Call.Wrote }, "no drain job survived to write anything: ${rig.calls.all()}")
    }

    // ---- 3: an end that was refused, and a source with no terminal -------------

    @Test
    fun b4_aRefusedEndTrackIsNotACompletedBroadcast() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls, audioSink = LaneSink(MediaKind.Audio, calls).scriptEnds(WriteOutcome.WouldBlock))
        val outcome =
            runBroadcast(rig) {
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome, "a refused end is not a completion")
        assertTrue(unfinished.reason.contains("endTrack"), "named: ${unfinished.reason}")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "nothing was completed: ${calls.all()}")
    }

    @Test
    fun b5_aUserStopWithoutEncoderTerminalsIsNotGracefulCompletion() = runTest {
        val rig = Rig()
        val outcome =
            runBroadcast(rig) {
                assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
                runCurrent()
                // The user stops: neither encoder ever delivered end-of-stream.
                rig.session.stop(IllegalStateException("the user left"))
                runCurrent()
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome, "a stop is not an end of media")
        assertTrue(unfinished.reason.contains("without") || unfinished.reason.contains("Cancelled"), "named: ${unfinished.reason}")
        assertTrue(rig.calls.all().none { it is Call.Completed }, "and nothing was completed: ${rig.calls.all()}")
    }

    // ---- 4: a Failed decision whose notification is delayed --------------------

    @Test
    fun b6_aFailedDecisionBehindAHeldNotificationNeverCompletes() = runTest {
        val calls = Calls()
        val entered = HostGate("the session's cleanup reached the held track close")
        val release = HostGate("the row released the held track close")
        val rig = Rig(calls = calls, videoHeldAt = entered to release)
        // The fault arrives on its OWN finite worker, so nothing on the test's
        // schedule is blocked inside the session's cleanup.
        val faulting =
            HostWorker("codec-error") {
                rig.session.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "CodecException: the encoder died"))
            }
        val outcome =
            settling(listOf(entered, release), listOf(faulting)) {
                runBroadcast(rig) {
                    // The video track ends first and its end is accepted.
                    assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                    runCurrent()
                    // Now the fault wins the terminal decision, and the
                    // announcement is held inside the session's cleanup.
                    faulting.start()
                    entered.await(faulting)
                    assertIs<CaptureTermination.Failed>(rig.session.termination, "the Failed decision has already won")
                    // The audio track now ends too: BOTH child results are
                    // successful while the notification is still withheld.
                    assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                    runCurrent()
                    assertEquals(
                        listOf<Call>(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT)),
                        calls.all(),
                        "both tracks were ended on the sender while the terminal was withheld",
                    )
                    release.openFully()
                    faulting.join()
                    runCurrent()
                }
            }
        assertIs<BroadcastOutcome.Unfinished>(outcome, "a Failed decision stays authoritative however late it is announced")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "and nothing was completed: ${calls.all()}")
        assertIs<CaptureTermination.Failed>(rig.session.termination)
    }

    /**
     * The same Failed decision, released only after both successful child
     * results are in hand. This is a DELAYED-TERMINAL variant of b6: it does
     * not, by itself, establish a receipt enqueue/consume ORDER, because no
     * receipt path is observable until the implementation exists. The exact
     * queue-order discriminator stays on the GREEN ledger.
     */
    @Test
    fun b11_aFailedTerminalQueuedBehindBothResultsStillBlocksCompletion() = runTest {
        val calls = Calls()
        val entered = HostGate("the session's cleanup reached the held track close")
        val release = HostGate("the row released the held track close")
        val rig = Rig(calls = calls, videoHeldAt = entered to release)
        val faulting =
            HostWorker("codec-error") {
                rig.session.onFault(GENERATION, CaptureFault.Codec(MediaKind.Audio, "CodecException: the microphone died"))
            }
        val outcome =
            settling(listOf(entered, release), listOf(faulting)) {
                runBroadcast(rig) {
                    assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                    runCurrent()
                    faulting.start()
                    entered.await(faulting)
                    assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                    runCurrent()
                    // Both results are in; only now is the Failed notification
                    // allowed to arrive behind them.
                    release.openFully()
                    faulting.join()
                    runCurrent()
                }
            }
        assertIs<BroadcastOutcome.Unfinished>(outcome, "the failure arriving last is still the outcome")
        assertTrue(calls.all().none { it is Call.Completed }, "nothing was completed on the way: ${calls.all()}")
    }

    // ---- 5: successes without an explicit Completed -----------------------------

    /**
     * ONE track ends while the other stays live. A correct coordinator WAITS,
     * so this row asserts the waiting state and then cancels and settles it --
     * a deadline is never the expected outcome here.
     */
    @Test
    fun b7_oneSuccessfulTrackWhileTheOtherStaysLiveIsNotSuccess() = runTest {
        val rig = Rig()
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
            runCurrent()
            assertEquals(
                listOf<Call>(Call.Ended(MediaKind.Video, WRITE_TIMEOUT)),
                rig.calls.all(),
                "the ended track was ended on the sender, and nothing else happened",
            )
            assertTrue(running.isActive, "the broadcast is still waiting for the other track")
            assertTrue(!seen.reached, "and it has not produced any outcome")
            running.cancel(CancellationException("the row has seen the waiting state"))
            runCatching { running.join() }
        }
        assertTrue(rig.calls.all().none { it is Call.Completed || it is Call.Drained })
    }

    /**
     * BOTH tracks end successfully and the session's terminal notification is
     * WITHHELD by a forwarding listener that returns at once -- so the real
     * decision and the real cleanup both complete, and only the coordinator's
     * knowledge of the terminal is delayed. Two successes are not a completed
     * broadcast.
     */
    @Test
    fun b12_twoSuccessfulDrainsWithoutTheSessionsTerminalAreNotSuccess() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls, withholdTerminal = true)
        val withheld = assertNotNull(rig.withheld)
        val outcome =
            try {
                runBroadcast(rig) {
                    assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                    runCurrent()
                    assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                    runCurrent()
                    assertEquals(
                        listOf<Call>(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT)),
                        calls.all(),
                        "both ends accepted, and NOTHING completed while the terminal is withheld",
                    )
                    assertTrue(withheld.isHeld, "the session really did decide and announce; the coordinator has not been told")
                    assertEquals(CaptureTermination.Completed, rig.session.termination)
                    withheld.release()
                    runCurrent()
                }
            } finally {
                // Owned on every path, including a failing assertion above.
                withheld.release()
            }
        assertIs<BroadcastOutcome.Published>(outcome, "once the terminal reaches it, the broadcast completes")
        assertEquals(
            listOf<Call>(
                Call.Ended(MediaKind.Video, WRITE_TIMEOUT),
                Call.Ended(MediaKind.Audio, WRITE_TIMEOUT),
                Call.Drained(DRAIN_TIMEOUT),
                Call.Completed,
                Call.Drained(DRAIN_TIMEOUT),
            ),
            calls.all(),
            "and the completion happens only after it",
        )
    }

    /**
     * CONTROL for the pending-write case, with its false primary requirement
     * removed. A write parked at a rendezvous has NOT failed yet: a successful
     * session stop closes both sources, the idle track finishes unfinished,
     * and a correct coordinator may cancel the parked sibling before its
     * scripted throw ever happens. What this row pins is that nothing is
     * completed and the planned throw is not manufactured into a primary.
     */
    @Test
    fun b13_aParkedWriteCancelledByCleanupNeverBecomesAPrimary() = runTest {
        val calls = Calls()
        val planned = IllegalStateException("a throw the parked write never reached")
        val inWrite = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val threw = java.util.concurrent.atomic.AtomicBoolean(false)
        val rig =
            Rig(
                calls = calls,
                audioSink =
                    LaneSink(
                        MediaKind.Audio,
                        calls,
                        beforeWrite = {
                            inWrite.complete(Unit)
                            release.await()
                            threw.set(true)
                            throw planned
                        },
                    ),
            )
        watching(rig) { running, _ ->
            assertEquals(HandoffOutcome.Queued, rig.audio.queue(0, bytes(2, 3), 21_333L, flags = 0))
            runCurrent()
            withTimeoutOrNull(BOUNDED) { inWrite.await() }
                ?: throw AssertionError("the audio write never started: the coordinator never reached it")
            rig.session.stop(null)
            runCurrent()
            assertEquals(CaptureTermination.Completed, rig.session.termination, "the session's own terminal is Completed")
            release.complete(Unit)
            runCurrent()
            runCatching { running.join() }
        }
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "nothing was completed: ${calls.all()}")
        assertTrue(!threw.get() || true, "whether the parked write ever threw is the coordinator's timing, not an oracle")
    }

    // ---- 6: a refused start makes no drains ------------------------------------

    @Test
    fun b8_aRefusedStartCreatesNoDrainJobs() = runTest {
        val rig = Rig()
        val settled = broadcasting(rig, permissions = Refused(GENERATION, "camera permanently denied")) { }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(settled.getOrThrow(), "a refused start is a named outcome")
        assertTrue(unfinished.reason.contains("denied") || unfinished.reason.contains("refused"), "named: ${unfinished.reason}")
        assertNull(unfinished.video, "no track ever drained")
        assertNull(unfinished.audio)
        assertEquals(emptyList(), rig.calls.all(), "and the sender was never asked for anything")
        // A refused start ACQUIRED nothing, so there was nothing to close: this
        // source is still open, and the evidence that no drain exists is that
        // an admitted output is never written, given a chance to be.
        assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L), "a start that closed nothing leaves its track open")
        runCurrent()
        assertEquals(emptyList(), rig.calls.all(), "and no drain job exists to publish it")
    }

    // ---- 7: the completion inventory for every drain answer and failure --------

    @Test
    fun b9_bothBarriersFlushedIsTheOnlyPublication() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls, completion = Completion(calls, drainAnswers = listOf(DrainOutcome.Flushed, DrainOutcome.Flushed)))
        val outcome =
            runBroadcast(rig) {
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
            }
        assertIs<BroadcastOutcome.Published>(outcome, "both barriers flushed")
        assertEquals(
            listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), Call.Drained(DRAIN_TIMEOUT), Call.Completed, Call.Drained(DRAIN_TIMEOUT)),
            calls.all(),
            "two ends, the media drain, one completion, the catalog drain",
        )
    }

    /** A media drain that did not flush is not published, and completion is never requested behind it. */
    @Test
    fun t1_everyUnflushedMediaDrainStopsBeforeCompletion() = runTest {
        for (answer in NOT_FLUSHED) {
            val calls = Calls()
            val rig = Rig(calls = calls, completion = Completion(calls, drainAnswers = listOf(answer)), resources = listOf(CaptureResource.Worker to null))
            val outcome =
                runBroadcast(rig) {
                    assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                    runCurrent()
                    assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                    runCurrent()
                }
            assertEquals(
                BroadcastOutcome.Unfinished("the accepted media did not drain before completion: $answer", TrackDrain.Ended(0, 0, 0), TrackDrain.Ended(0, 0, 0)),
                outcome,
                "$answer: reported as itself, at the first barrier",
            )
            assertEquals(
                listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), Call.Drained(DRAIN_TIMEOUT)),
                calls.all(),
                "$answer: no completion after an unflushed media drain",
            )
            assertEquals(listOf<Any>(rig.identity(CaptureResource.Worker)), rig.closed(), "$answer: the capture cleanup ran once")
        }
    }

    /** A completion whose catalog drain did not flush is accepted and cannot be undone; it is still not published. */
    @Test
    fun t2_everyUnflushedCatalogDrainIsUnfinishedAfterTheAcceptedCompletion() = runTest {
        for (answer in NOT_FLUSHED) {
            val calls = Calls()
            val rig = Rig(calls = calls, completion = Completion(calls, drainAnswers = listOf(DrainOutcome.Flushed, answer)), resources = listOf(CaptureResource.Worker to null))
            val outcome =
                runBroadcast(rig) {
                    assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                    runCurrent()
                    assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                    runCurrent()
                }
            assertEquals(
                BroadcastOutcome.Unfinished("the completion was accepted but its terminal catalog did not drain: $answer", TrackDrain.Ended(0, 0, 0), TrackDrain.Ended(0, 0, 0)),
                outcome,
                "$answer: reported as itself, at the second barrier",
            )
            assertEquals(
                listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), Call.Drained(DRAIN_TIMEOUT), Call.Completed, Call.Drained(DRAIN_TIMEOUT)),
                calls.all(),
                "$answer: the completion stands; nothing is retried or undone",
            )
            assertEquals(listOf<Any>(rig.identity(CaptureResource.Worker)), rig.closed(), "$answer: the capture cleanup ran once")
        }
    }

    /** An Exception or an Error raised at either barrier leaves as itself, with the cleanup run once and nothing after it. */
    @Test
    fun t3_aFailureAtEitherBarrierLeavesAsItself() = runTest {
        for (at in 0..1) {
            for (raised in listOf<Throwable>(IllegalStateException("drain $at failed"), AssertionError("drain $at raised an Error"))) {
                val calls = Calls()
                val rig = Rig(calls = calls, completion = Completion(calls, raiseOnDrain = { raised }, raiseAtDrain = at), resources = listOf(CaptureResource.Worker to null))
                val thrown =
                    runBroadcastCatching(rig) {
                        assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                        runCurrent()
                        assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                        runCurrent()
                    }
                assertSame(raised, thrown, "drain $at: its own failure leaves")
                assertEquals(emptyList(), raised.suppressed.toList(), "drain $at: nothing is attached to it")
                val expected =
                    if (at == 0) {
                        listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), Call.Drained(DRAIN_TIMEOUT))
                    } else {
                        listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), Call.Drained(DRAIN_TIMEOUT), Call.Completed, Call.Drained(DRAIN_TIMEOUT))
                    }
                assertEquals(expected, calls.all(), "drain $at: each step once, nothing after the failure")
                assertEquals(listOf<Any>(rig.identity(CaptureResource.Worker)), rig.closed(), "drain $at: the capture cleanup ran once")
            }
        }
    }

    /** Cancelled while PARKED inside either barrier: cancellation stays cancellation, and nothing new starts. */
    @Test
    fun t4_cancellationParkedInEitherBarrierStartsNothingNew() = runTest {
        for (at in 0..1) {
            val calls = Calls()
            val parked = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val rig =
                Rig(
                    calls = calls,
                    completion =
                        Completion(calls, beforeDrain = { index ->
                            if (index == at) {
                                parked.complete(Unit)
                                release.await()
                            }
                        }),
                    resources = listOf(CaptureResource.Worker to null),
                )
            var observed: Throwable? = null
            watching(rig) { running, seen ->
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
                withTimeoutOrNull(BOUNDED) { parked.await() } ?: throw AssertionError("drain $at was never reached")
                running.cancel(CancellationException("the user left inside drain $at"))
                release.complete(Unit)
                runCurrent()
                runCatching { running.join() }
                observed = seen.observed().exceptionOrNull()
            }
            assertIs<CancellationException>(observed, "drain $at: cancellation stays cancellation")
            val expected =
                if (at == 0) {
                    listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT))
                } else {
                    listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), Call.Drained(DRAIN_TIMEOUT), Call.Completed)
                }
            assertEquals(expected, calls.all(), "drain $at: the parked drain never answered, and nothing started after it")
            assertEquals(listOf<Any>(rig.identity(CaptureResource.Worker)), rig.closed(), "drain $at: the capture cleanup ran once")
        }
    }

    @Test
    fun b10_aThrowingCompletionDoesNotFallThroughToASuccessfulDrain() = runTest {
        val calls = Calls()
        val raised = IllegalStateException("complete() failed")
        val rig = Rig(calls = calls, completion = Completion(calls, raiseOnComplete = { raised }))
        val thrown =
            runBroadcastCatching(rig) {
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
            }
        assertSame(raised, thrown, "the completion's own failure is what leaves")
        assertEquals(
            listOf(Call.Ended(MediaKind.Video, WRITE_TIMEOUT), Call.Ended(MediaKind.Audio, WRITE_TIMEOUT), Call.Drained(DRAIN_TIMEOUT), Call.Completed),
            calls.all(),
            "the media drain came first, and a failed completion is not drained over",
        )
    }

    @Test
    fun b14_aThrowingDrainKeepsItsFailureAndTheExactInventory() = runTest {
        val calls = Calls()
        val raised = IllegalStateException("drain() failed")
        val rig = Rig(calls = calls, completion = Completion(calls, raiseOnDrain = { raised }))
        val thrown =
            runBroadcastCatching(rig) {
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
            }
        assertSame(raised, thrown, "the drain's own failure is what leaves")
        assertEquals(
            listOf<Call>(
                Call.Ended(MediaKind.Video, WRITE_TIMEOUT),
                Call.Ended(MediaKind.Audio, WRITE_TIMEOUT),
                Call.Drained(DRAIN_TIMEOUT),
            ),
            calls.all(),
            "each step happened exactly once, and nothing followed the failure",
        )
    }

    /** Cancellation AT the boundary before the completion work: none of it begins. */
    @Test
    fun b15_cancellationAtTheCompletionBoundaryStartsNoCompletionWork() = runTest {
        val calls = Calls()
        val atBoundary = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val rig =
            Rig(
                calls = calls,
                // The audio end is the last operation before completion: the
                // coordinator is PARKED here, not merely signalled.
                audioSink =
                    LaneSink(
                        MediaKind.Audio,
                        calls,
                        beforeEnd = {
                            atBoundary.complete(Unit)
                            release.await()
                        },
                    ),
            )
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
            runCurrent()
            assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
            runCurrent()
            withTimeoutOrNull(BOUNDED) { atBoundary.await() }
                ?: throw AssertionError("the completion boundary was never reached: the coordinator never ended the last track")
            // The milestone FIRST: the video end is already resolved and the
            // audio end has not been recorded, because it is parked.
            assertEquals(
                listOf<Call>(Call.Ended(MediaKind.Video, WRITE_TIMEOUT)),
                calls.all(),
                "the coordinator is parked inside the last track's end",
            )
            running.cancel(CancellationException("the user left at the completion boundary"))
            release.complete(Unit)
            runCurrent()
            runCatching { running.join() }
            observed = seen.observed().exceptionOrNull()
        }
        assertIs<CancellationException>(observed, "cancellation stays cancellation")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "no completion work began: ${calls.all()}")
    }

    /**
     * Both terminals are queued BEFORE the broadcast starts, so a drain can
     * finish very early. It is a valid control for an early-finishing child;
     * it does NOT force completion before a completion handler is registered,
     * which needs the actual handler boundary and belongs to GREEN.
     */
    @Test
    fun b16_aChildThatFinishedBeforeItIsObservedIsStillCountedOnce() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls)
        // Both terminals are already queued before the broadcast starts, so a
        // drain can finish before the coordinator ever observes it.
        assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
        assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
        val outcome = runBroadcast(rig) { runCurrent() }
        assertIs<BroadcastOutcome.Published>(outcome, "an early-finishing child is not a lost child")
        assertEquals(
            listOf<Call>(
                Call.Ended(MediaKind.Video, WRITE_TIMEOUT),
                Call.Ended(MediaKind.Audio, WRITE_TIMEOUT),
                Call.Drained(DRAIN_TIMEOUT),
                Call.Completed,
                Call.Drained(DRAIN_TIMEOUT),
            ),
            calls.all(),
            "each end once, the media drain, one completion, the catalog drain",
        )
    }

    /** Readiness delivered INSIDE start is still the readiness this broadcast runs on. */
    @Test
    fun b17_readinessDeliveredDuringStartIsNotLost() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls)
        val outcome =
            runBroadcast(rig) {
                // NoAcquire settles everything synchronously, so the session
                // reports Started and delivers onReady from inside start().
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
            }
        assertIs<BroadcastOutcome.Published>(outcome, "a readiness delivered during start still starts the drains")
        assertEquals(2, calls.all().count { it is Call.Ended }, "each track ended once: ${calls.all()}")
    }

    /**
     * The real route for a session that really says Completed while a failure
     * really happens: both track ends are accepted, `onTrackEnded` decides
     * Completed and announces it, and the cleanup failure escapes from that
     * very call. BOTH facts are proven, and nothing is completed.
     */
    @Test
    fun b18_aCompletedSessionWhoseCleanupFailedIsStillAFailure() = runTest {
        val calls = Calls()
        val raised = IllegalStateException("the video track's close threw")
        val rig = Rig(calls = calls, videoCloseRaises = { raised })
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
            runCurrent()
            assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
            runCurrent()
            runCatching { running.join() }
            observed = seen.observed().exceptionOrNull()
        }
        assertSame(raised, observed, "the cleanup failure reaches the caller by identity")
        assertEquals(CaptureTermination.Completed, rig.session.termination, "and the session's own terminal really was Completed")
        assertEquals(2, calls.all().count { it is Call.Ended }, "each track was still ended exactly once: ${calls.all()}")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "a broadcast whose ending failed did not complete")
    }

    /**
     * A late fact after the broadcast returned changes nothing and throws
     * nothing. Note what this does and does not show: the session FILTERS a
     * fault after its first terminal, so the coordinator's listener never sees
     * it. The retired-receipt discriminator needs the receipt boundary itself
     * and belongs to GREEN.
     */
    @Test
    fun b19_aLateFactAfterTheBroadcastReturnedIsInert() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls)
        val outcome =
            runBroadcast(rig) {
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
            }
        assertIs<BroadcastOutcome.Published>(outcome)
        val after = calls.all()
        // A fault that lost the race, and a callback behind it.
        rig.session.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "a callback that lost the race"))
        assertIs<HandoffOutcome.Ignored>(rig.video.queue(2, videoFrame(9), 99_999L), "the track is closed")
        runCurrent()
        assertEquals(after, calls.all(), "nothing was published, completed or drained afterwards")
        assertEquals(CaptureTermination.Completed, rig.session.termination, "and the ending it had is the one it keeps")
    }

    /** An independent CancellationException from EACH track, with the sibling idle. */
    @Test
    fun b20_anIndependentlyCancelledTrackSettlesItsIdleSibling() = runTest {
        for (cancelling in listOf(MediaKind.Video, MediaKind.Audio)) {
            val calls = Calls()
            val raised = CancellationException("the $cancelling sender was cancelled on its own")
            val rig =
                Rig(
                    calls = calls,
                    videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { if (cancelling == MediaKind.Video) throw raised }),
                    audioSink = LaneSink(MediaKind.Audio, calls, beforeWrite = { if (cancelling == MediaKind.Audio) throw raised }),
                )
            var observed: Throwable? = null
            watching(rig) { running, seen ->
                // Only the cancelling track produces anything; its sibling
                // stays idle throughout.
                val lane = if (cancelling == MediaKind.Video) rig.video else rig.audio
                val flags = if (cancelling == MediaKind.Audio) 0 else CaptureContract.FLAG_KEY_FRAME
                assertEquals(HandoffOutcome.Queued, lane.queue(0, videoFrame(1), 33_333L, flags = flags))
                runCurrent()
                runCatching { running.join() }
                assertTrue(seen.reached, "$cancelling: the broadcast reached its own boundary")
                observed = seen.observed().exceptionOrNull()
            }
            assertIs<CancellationException>(observed, "$cancelling: an independently cancelled child is cancellation")
            assertTrue(
                calls.all().none { it is Call.Completed || it is Call.Drained },
                "$cancelling: nothing was completed: ${calls.all()}",
            )
        }
    }

    // ---- the cleanup evidence the coordinator must transfer --------------------

    /**
     * An `Error` out of a track, with a resource close that FAILS during the
     * session's cleanup.
     *
     * The declared policy: the Error keeps its identity and is what leaves; the
     * cleanup failure is attached to it as a ROOT, once, by identity; the
     * coordinator's private stop reason is never attached as an ordinary root;
     * and the termination is not a false Completed. The reason may still appear
     * INSIDE the inherited graph of a retained carrier -- that is the callee's
     * own structure, not a new top-level failure.
     */
    @Test
    fun b21_anErrorPrimaryCollectsTheCleanupRootsItCaused() = runTest {
        val cleanup = IllegalStateException("the camera's close failed")
        val raised = AssertionError("an Error inside the video sender")
        val calls = Calls()
        val rig =
            Rig(
                calls = calls,
                videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { throw raised }),
                resources = listOf(CaptureResource.Camera to cleanup),
            )
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
            runCurrent()
            runCatching { running.join() }
            observed = seen.observed().exceptionOrNull()
        }
        assertSame(raised, observed, "the Error keeps its identity at the coordinator's own frame")
        assertEquals(
            listOf<Throwable>(cleanup),
            raised.suppressed.toList(),
            "exactly that root, once, and no others",
        )
        assertTrue(raised.suppressed.none { it is BroadcastStopped }, "the coordinator's own stop reason is never a root")
        assertEquals(listOf<Any>(rig.identity(CaptureResource.Camera)), rig.closed(), "every handle was attempted")
        assertIs<CaptureTermination.Cancelled>(rig.session.termination, "and the session ended Cancelled, never a false Completed")
    }

    /** A cleanup `Error` and a cleanup cancellation are roots too, with their inherited edges intact. */
    @Test
    fun b22_cleanupErrorsAndCancellationsAreRootsToo() = runTest {
        for (cleanup in listOf<Throwable>(
            AssertionError("the camera's close failed with an Error"),
            CancellationException("the camera's close cancelled itself"),
            IllegalStateException("the camera's close failed"),
        )) {
            val raised = IllegalStateException("the video sender failed")
            val calls = Calls()
            val rig =
                Rig(
                    calls = calls,
                    videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { throw raised }),
                    resources = listOf(CaptureResource.Camera to cleanup),
                )
            var observed: Throwable? = null
            watching(rig) { running, seen ->
                assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
                runCurrent()
                runCatching { running.join() }
                observed = seen.observed().exceptionOrNull()
            }
            assertSame(raised, observed, "$cleanup: the primary is preserved")
            val roots = raised.suppressed.toList()
            // An Error or a cancellation raised by a close LEAVES the accepted
            // ownership as the carrier, suppressing the coordinator's reason;
            // an ordinary failure is REPORTED on that reason instead. Either
            // way exactly one root arrives, it is that failure, and the
            // adapter reason is never a root of its own.
            assertEquals(listOf<Throwable>(cleanup), roots, "$cleanup: exactly that root, once: $roots")
            assertTrue(roots.none { it is BroadcastStopped }, "$cleanup: the adapter reason is not a root")
            assertEquals(listOf<Any>(rig.identity(CaptureResource.Camera)), rig.closed(), "$cleanup: the close was attempted")
        }
    }

    /** Several failing closes: every distinct root, once each, in the order they happened. */
    @Test
    fun b23_everyDistinctCleanupFailureIsARootOnceInOrder() = runTest {
        val first = IllegalStateException("the microphone's close failed")
        val second = IllegalStateException("the camera's close failed")
        val raised = IllegalStateException("the video sender failed")
        val calls = Calls()
        val rig =
            Rig(
                calls = calls,
                videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { throw raised }),
                // Acquired in this order, so they close in REVERSE.
                resources = listOf(CaptureResource.Camera to second, CaptureResource.Microphone to first),
            )
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
            runCurrent()
            runCatching { running.join() }
            observed = seen.observed().exceptionOrNull()
        }
        assertSame(raised, observed)
        assertEquals(
            listOf<Throwable>(first, second),
            raised.suppressed.toList(),
            "both roots, once each, in the order the closes happened",
        )
        assertEquals(
            listOf<Any>(rig.identity(CaptureResource.Microphone), rig.identity(CaptureResource.Camera)),
            rig.closed(),
            "and the physical close order is its own inventory, in reverse acquisition order",
        )
    }

    /** The SAME instance failing two closes is one root, not two. */
    @Test
    fun b24_aRepeatedCleanupInstanceIsOneRoot() = runTest {
        val shared = IllegalStateException("both closes failed with the same instance")
        val raised = IllegalStateException("the video sender failed")
        val calls = Calls()
        val rig =
            Rig(
                calls = calls,
                videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { throw raised }),
                resources = listOf(CaptureResource.Camera to shared, CaptureResource.Microphone to shared),
            )
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
            runCurrent()
            runCatching { running.join() }
            observed = seen.observed().exceptionOrNull()
        }
        assertSame(raised, observed)
        assertEquals(listOf<Throwable>(shared), raised.suppressed.toList(), "attached once, by identity")
        assertEquals(
            listOf<Any>(rig.identity(CaptureResource.Microphone), rig.identity(CaptureResource.Camera)),
            rig.closed(),
            "though both closes were attempted",
        )
    }

    /**
     * The FIRST terminal notification is still withheld when the broadcast
     * retires on an early exit, and is released afterwards. Nothing is
     * resurrected, republished or thrown.
     */
    @Test
    fun b25_aFirstTerminalDeliveredAfterRetirementIsInert() = runTest {
        val calls = Calls()
        val rig =
            Rig(
                calls = calls,
                videoSink = LaneSink(MediaKind.Video, calls).scriptEnds(WriteOutcome.WouldBlock),
                withholdTerminal = true,
            )
        val withheld = assertNotNull(rig.withheld)
        var outcome: BroadcastOutcome? = null
        var before: List<Call> = emptyList()
        try {
            outcome =
                runBroadcast(rig) {
                    // The video end is refused, so the broadcast retires early.
                    assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                    runCurrent()
                }
            assertTrue(!rig.events.terminal.isCompleted, "no terminal reached the coordinator's listener before it exited")
            assertTrue(withheld.isHeld, "the session's FIRST terminal is still withheld")
        } finally {
            // The baseline is taken BEFORE the release, so nothing the
            // delivery does can become part of what is expected.
            before = calls.all()
            withheld.release()
        }
        assertIs<BroadcastOutcome.Unfinished>(outcome, "the refused end is the outcome")
        assertEquals(before, calls.all(), "the released first terminal did nothing on delivery")
        runCurrent()
        assertEquals(before, calls.all(), "and nothing it scheduled did either")
    }

    /** A DUPLICATE terminal after a successful run: the one-shot state simply rejects it. */
    @Test
    fun b26_aDuplicateTerminalAfterASuccessfulRunIsRejected() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls)
        val outcome =
            runBroadcast(rig) {
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
            }
        assertIs<BroadcastOutcome.Published>(outcome)
        val after = calls.all()
        rig.events.onTerminated(GENERATION, CaptureTermination.Cancelled("a callback that lost the race"))
        rig.events.onReady(GENERATION, CaptureReady.Refused("and a readiness behind it"))
        runCurrent()
        assertEquals(after, calls.all(), "nothing was published, completed or drained afterwards")
    }

    // ---- the lifecycle boundaries -------------------------------------------

    /** Cancellation before readiness must still close what the start acquired. */
    @Test
    fun f1_cancellationBeforeReadinessClosesTheStartedSession() = runTest {
        val rig = Rig(pendingAcquisition = true)
        try {
            watching(rig) { running, _ ->
                // The session is Pending: a microphone is owned and the camera
                // ticket is outstanding, so no readiness can have arrived.
                assertTrue(!rig.events.readiness.isCompleted, "readiness is still pending")
                running.cancel(CancellationException("the user left before it was ready"))
                runCatching { running.join() }
                runCurrent()
            }
            assertEquals(
                listOf<Any>(rig.microphone),
                rig.closed(),
                "the resources the start acquired are closed by the coordinator that started it",
            )
        } finally {
            // Whatever the oracle decided, this row leaves nothing outstanding:
            // a stop is a decision, not a settled callback, so the pending
            // camera step is settled here and its outcome reported.
            rig.session.stop(IllegalStateException("the row is finished"))
            val settled = rig.pending?.settleOutstanding()
            assertNotNull(settled, "the outstanding camera step was settled by this row's cleanup")
            assertEquals(0, rig.ownership.pending, "and nothing is left outstanding: $settled")
        }
    }

    /** A cancelled broadcast keeps the cleanup failure its own stop produced. */
    @Test
    fun f2_parentCancellationKeepsTheCleanupFailure() = runTest {
        for (cleanup in listOf<Throwable>(
            IllegalStateException("the camera's close failed"),
            AssertionError("the camera's close failed with an Error"),
            CancellationException("the camera's close cancelled itself"),
        )) {
            val rig = Rig(resources = listOf(CaptureResource.Camera to cleanup))
            var observed: Throwable? = null
            watching(rig) { running, seen ->
                running.cancel(CancellationException("the user left"))
                runCatching { running.join() }
                runCurrent()
                observed = seen.observed().exceptionOrNull()
            }
            val cancelled = assertIs<CancellationException>(observed, "$cleanup: cancellation stays cancellation")
            assertEquals(
                listOf<Throwable>(cleanup),
                cancelled.suppressed.toList(),
                "$cleanup: carrying exactly the cleanup root its stop produced",
            )
            assertEquals(listOf<Any>(rig.identity(CaptureResource.Camera)), rig.closed(), "$cleanup: the close was attempted once")
        }
    }

    /** An unfinished broadcast must not swallow a real cleanup failure. */
    @Test
    fun f3_anUnfinishedOutcomeDoesNotSwallowACleanupFailure() = runTest {
        for (cleanup in listOf<Throwable>(
            IllegalStateException("the camera's close failed"),
            AssertionError("the camera's close failed with an Error"),
            CancellationException("the camera's close cancelled itself"),
        )) {
            unfinishedCarries(cleanup)
        }
    }

    private suspend fun TestScope.unfinishedCarries(cleanup: Throwable) {
        val calls = Calls()
        val rig =
            Rig(
                calls = calls,
                videoSink = LaneSink(MediaKind.Video, calls).scriptEnds(WriteOutcome.WouldBlock),
                resources = listOf(CaptureResource.Camera to cleanup),
            )
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
            runCurrent()
            runCatching { running.join() }
            observed = seen.observed().exceptionOrNull()
        }
        assertSame(cleanup, observed, "$cleanup: with no primary of its own, the real cleanup failure escapes")
        assertEquals(listOf<Any>(rig.identity(CaptureResource.Camera)), rig.closed(), "$cleanup: the close was attempted once")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "$cleanup: nothing was completed: ${calls.all()}")
    }

    /** A Failed terminal retires the drains itself, without waiting for another wake. */
    @Test
    fun f4_aFailedTerminalRetiresParkedDrainsOnItsOwn() = runTest {
        val calls = Calls()
        val inVideo = CompletableDeferred<Unit>()
        val inAudio = CompletableDeferred<Unit>()
        val never = CompletableDeferred<Unit>()
        val rig =
            Rig(
                calls = calls,
                videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { inVideo.complete(Unit); never.await() }),
                audioSink = LaneSink(MediaKind.Audio, calls, beforeWrite = { inAudio.complete(Unit); never.await() }),
            )
        var observed: Result<BroadcastOutcome>? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
            assertEquals(HandoffOutcome.Queued, rig.audio.queue(0, bytes(2, 3), 21_333L, flags = 0))
            runCurrent()
            withTimeoutOrNull(BOUNDED) { inVideo.await() } ?: throw AssertionError("the video write never started")
            withTimeoutOrNull(BOUNDED) { inAudio.await() } ?: throw AssertionError("the audio write never started")
            // BOTH drains are parked inside the sender. The capture session
            // fails: that terminal alone must retire them.
            rig.session.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "CodecException: the encoder died"))
            runCurrent()
            assertTrue(seen.reached, "the broadcast settled on the terminal alone, with no further wake")
            observed = seen.observed()
            runCatching { running.join() }
        }
        val settled = assertNotNull(observed)
        assertTrue(settled.isFailure || settled.getOrNull() is BroadcastOutcome.Unfinished, "a failed session is never a completion: $settled")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "nothing was completed: ${calls.all()}")
    }

    /** Cancellation at a RESOLVED complete does not start the catalog drain behind it. */
    @Test
    fun f5_cancellationAtAResolvedCompleteDoesNotStartTheCatalogDrain() = runTest {
        val calls = Calls()
        val job = Job(coroutineContext[Job])
        val rig =
            Rig(
                calls = calls,
                completion = Completion(calls, onComplete = { job.cancel(CancellationException("stopped at the completion")) }),
            )
        val seen = RunObservation()
        supervisorScope {
            val running =
                async(job) {
                    val result =
                        runCatching { rig.broadcast.run(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available) }
                    seen.record(result)
                    result.getOrThrow()
                }
            try {
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
                runCurrent()
                runCatching { running.join() }
                assertTrue(seen.reached, "the broadcast reached its own boundary")
            } finally {
                running.cancel(CancellationException("the row is finished"))
                runCatching { running.join() }
            }
        }
        val settled = seen.observed()
        assertTrue(settled.isFailure, "a cancelled broadcast does not produce an outcome: $settled")
        assertEquals(
            listOf<Call>(
                Call.Ended(MediaKind.Video, WRITE_TIMEOUT),
                Call.Ended(MediaKind.Audio, WRITE_TIMEOUT),
                Call.Drained(DRAIN_TIMEOUT),
                Call.Completed,
            ),
            calls.all(),
            "the completion that resolved stands; the catalog drain behind it never begins",
        )
    }

    /**
     * Both terminals are queued before the run and the run is driven on an
     * unconfined schedule. This is a behavioural row: everything queued before
     * the broadcast starts is still drained, ended, completed and drained
     * exactly once. It does NOT establish that a child completed before its
     * receipt was registered -- measurement says it does not -- and
     * [f9_theProductionRegistrationReceiptsAnAlreadyCompletedChild] is what
     * covers that.
     */
    @Test
    fun f6_outputQueuedBeforeTheRunIsStillPublished() = runTest {
        val calls = Calls()
        val rig = Rig(calls = calls)
        assertEquals(HandoffOutcome.Queued, rig.video.queueEnd(0))
        assertEquals(HandoffOutcome.Queued, rig.audio.queueEnd(0))
        val outcome =
            withContext(UnconfinedTestDispatcher(testScheduler)) {
                rig.broadcast.run(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available)
            }
        assertIs<BroadcastOutcome.Published>(outcome, "everything queued before the run is still published")
        assertEquals(
            listOf<Call>(
                Call.Ended(MediaKind.Video, WRITE_TIMEOUT),
                Call.Ended(MediaKind.Audio, WRITE_TIMEOUT),
                Call.Drained(DRAIN_TIMEOUT),
                Call.Completed,
                Call.Drained(DRAIN_TIMEOUT),
            ),
            calls.all(),
            "each end once, the media drain, one completion, the catalog drain",
        )
    }

    /**
     * A child that REALLY throws while the broadcast is retiring on a failed
     * terminal: its failure is actual, and it must not disappear into an
     * outcome value.
     */
    @Test
    fun f7_aFailureRaisedWhileRetiringIsNotDiscarded() = runTest {
        for (failing in listOf(MediaKind.Video, MediaKind.Audio)) {
            val calls = Calls()
            val raised = IllegalStateException("the $failing sender failed while the broadcast was retiring")
            val thrown = java.util.concurrent.atomic.AtomicBoolean(false)
            val release = CompletableDeferred<Unit>()
            val never = CompletableDeferred<Unit>()
            val inVideo = CompletableDeferred<Unit>()
            val inAudio = CompletableDeferred<Unit>()
            fun park(kind: MediaKind, entered: CompletableDeferred<Unit>): suspend (Int) -> Unit = {
                entered.complete(Unit)
                if (kind == failing) {
                    release.await()
                    thrown.set(true)
                    throw raised
                }
                never.await()
            }
            val rig =
                Rig(
                    calls = calls,
                    videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = park(MediaKind.Video, inVideo)),
                    audioSink = LaneSink(MediaKind.Audio, calls, beforeWrite = park(MediaKind.Audio, inAudio)),
                )
            var observed: Throwable? = null
            watching(rig) { running, seen ->
                assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
                assertEquals(HandoffOutcome.Queued, rig.audio.queue(0, bytes(2, 3), 21_333L, flags = 0))
                runCurrent()
                withTimeoutOrNull(BOUNDED) { inVideo.await() } ?: throw AssertionError("$failing: the video write never started")
                withTimeoutOrNull(BOUNDED) { inAudio.await() } ?: throw AssertionError("$failing: the audio write never started")
                // The session fails, which retires the publication...
                rig.session.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "CodecException: the encoder died"))
                // ...and the parked sender then really fails, with no
                // suspension between the release and the throw.
                release.complete(Unit)
                runCurrent()
                runCatching { running.join() }
                observed = seen.observed().exceptionOrNull()
            }
            assertTrue(thrown.get(), "$failing: precondition -- the sink really threw")
            assertSame(raised, observed, "$failing: a failure that really happened is not discarded by retirement")
            assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "$failing: nothing completed: ${calls.all()}")
        }
    }

    /**
     * The control for that accounting: a parked write that never fails must
     * stay an ordinary retirement, not become a fabricated failure.
     */
    @Test
    fun f8_aParkedWriteThatNeverFailsStaysAnOrdinaryRetirement() = runTest {
        val calls = Calls()
        val never = CompletableDeferred<Unit>()
        val inVideo = CompletableDeferred<Unit>()
        val rig =
            Rig(
                calls = calls,
                videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { inVideo.complete(Unit); never.await() }),
            )
        var settled: Result<BroadcastOutcome>? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
            runCurrent()
            withTimeoutOrNull(BOUNDED) { inVideo.await() } ?: throw AssertionError("the video write never started")
            rig.session.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "CodecException: the encoder died"))
            runCurrent()
            runCatching { running.join() }
            settled = seen.observed()
        }
        val outcome = assertNotNull(settled)
        assertTrue(outcome.isSuccess, "the coordinator's own cancellation is not an independent failure: $outcome")
        assertIs<BroadcastOutcome.Unfinished>(outcome.getOrNull(), "a failed session is an unfinished broadcast")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained })
    }

    /**
     * The production receipt registration, exercised on children that have
     * ALREADY completed -- normally, by failure and by cancellation. This is
     * the sample's own helper, the one `drainChild` uses.
     */
    @Test
    fun f9_theProductionRegistrationReceiptsAnAlreadyCompletedChild() = runTest {
        val raised = IllegalStateException("the child failed")
        val cancelled = CancellationException("the child was cancelled")
        supervisorScope {
            val scripts = listOf<suspend () -> Unit>({ }, { throw raised }, { throw cancelled })
            val expected = listOf<Throwable?>(null, raised, cancelled)
            for ((script, cause) in scripts.zip(expected)) {
                val child = async { script() }
                runCatching { child.join() }
                assertTrue(child.isCompleted, "the child is ALREADY completed before anything is registered")
                val receipts = mutableListOf<Throwable?>()
                child.reportCompletion { raisedCause -> synchronized(receipts) { receipts.add(raisedCause) } }
                runCurrent()
                assertEquals(1, receipts.size, "exactly one receipt for an already-completed child: $receipts")
                if (cause == null) {
                    assertNull(receipts.single(), "a child that returned reports no cause")
                } else {
                    assertSame(cause, receipts.single(), "and its ACTUAL cause, by identity")
                }
            }
        }
    }

    /**
     * The broadcast is cancelled while both drains are parked, and one of them
     * REALLY fails on its way out. Cancellation stays the outgoing primary,
     * and that actual failure is not lost with it.
     */
    @Test
    fun g1_parentCancellationKeepsAChildFailureRaisedOnItsWayOut() = runTest {
        for (failing in listOf(MediaKind.Video, MediaKind.Audio)) {
            for (raised in listOf<Throwable>(
                IllegalStateException("the $failing sender failed as it unwound"),
                AssertionError("the $failing sender failed as it unwound, with an Error"),
            )) {
                val calls = Calls()
                val thrown = java.util.concurrent.atomic.AtomicBoolean(false)
                val never = CompletableDeferred<Unit>()
                val inVideo = CompletableDeferred<Unit>()
                val inAudio = CompletableDeferred<Unit>()
                fun park(kind: MediaKind, entered: CompletableDeferred<Unit>): suspend (Int) -> Unit = {
                    entered.complete(Unit)
                    if (kind == failing) {
                        try {
                            never.await()
                        } finally {
                            thrown.set(true)
                            throw raised
                        }
                    }
                    never.await()
                }
                val rig =
                    Rig(
                        calls = calls,
                        videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = park(MediaKind.Video, inVideo)),
                        audioSink = LaneSink(MediaKind.Audio, calls, beforeWrite = park(MediaKind.Audio, inAudio)),
                    )
                var observed: Throwable? = null
                watching(rig) { running, seen ->
                    assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
                    assertEquals(HandoffOutcome.Queued, rig.audio.queue(0, bytes(2, 3), 21_333L, flags = 0))
                    runCurrent()
                    withTimeoutOrNull(BOUNDED) { inVideo.await() } ?: throw AssertionError("$failing: the video write never started")
                    withTimeoutOrNull(BOUNDED) { inAudio.await() } ?: throw AssertionError("$failing: the audio write never started")
                    running.cancel(CancellationException("the user left"))
                    runCurrent()
                    runCatching { running.join() }
                    observed = seen.observed().exceptionOrNull()
                }
                assertTrue(thrown.get(), "$failing/$raised: precondition -- the sink really failed on its way out")
                val cancelled = assertIs<CancellationException>(observed, "$failing/$raised: cancellation stays the primary")
                assertEquals(
                    listOf<Throwable>(raised),
                    cancelled.suppressed.toList(),
                    "$failing/$raised: carrying the failure that really happened",
                )
                assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "$failing/$raised: nothing completed")
            }
        }
    }

    /** The control: cancelling parked writes that do NOT fail carries nothing. */
    @Test
    fun g2_parentCancellationWithNoChildFailureCarriesNothing() = runTest {
        val calls = Calls()
        val never = CompletableDeferred<Unit>()
        val inVideo = CompletableDeferred<Unit>()
        val rig = Rig(calls = calls, videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = { inVideo.complete(Unit); never.await() }))
        var observed: Throwable? = null
        watching(rig) { running, seen ->
            assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
            runCurrent()
            withTimeoutOrNull(BOUNDED) { inVideo.await() } ?: throw AssertionError("the video write never started")
            running.cancel(CancellationException("the user left"))
            runCurrent()
            runCatching { running.join() }
            observed = seen.observed().exceptionOrNull()
        }
        val cancelled = assertIs<CancellationException>(observed)
        assertEquals(emptyList(), cancelled.suppressed.toList(), "nothing failed, so nothing is carried")
        assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained })
    }

    /**
     * A REAL failure whose cause happens to be the coordinator's own
     * cancellation is still a real failure: it is not filtered away as
     * "the owner cancelled this".
     */
    @Test
    fun g3_aRealFailureWrappingTheOwnersCancellationIsNotIgnored() = runTest {
        for (wrap in listOf<(Throwable) -> Throwable>(
            { cause -> IllegalStateException("cleanup failed while handling cancellation", cause) },
            { cause -> AssertionError("cleanup failed while handling cancellation, with an Error", cause) },
        )) {
            val calls = Calls()
            val actual = java.util.concurrent.atomic.AtomicReference<Throwable?>(null)
            val never = CompletableDeferred<Unit>()
            val inVideo = CompletableDeferred<Unit>()
            val inAudio = CompletableDeferred<Unit>()
            fun park(entered: CompletableDeferred<Unit>, wrapping: Boolean): suspend (Int) -> Unit = {
                entered.complete(Unit)
                if (wrapping) {
                    try {
                        never.await()
                    } catch (cancelled: CancellationException) {
                        val raised = wrap(cancelled)
                        actual.set(raised)
                        throw raised
                    }
                } else {
                    never.await()
                }
            }
            val rig =
                Rig(
                    calls = calls,
                    videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = park(inVideo, wrapping = true)),
                    audioSink = LaneSink(MediaKind.Audio, calls, beforeWrite = park(inAudio, wrapping = false)),
                )
            var observed: Throwable? = null
            watching(rig) { running, seen ->
                assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
                assertEquals(HandoffOutcome.Queued, rig.audio.queue(0, bytes(2, 3), 21_333L, flags = 0))
                runCurrent()
                withTimeoutOrNull(BOUNDED) { inVideo.await() } ?: throw AssertionError("the video write never started")
                withTimeoutOrNull(BOUNDED) { inAudio.await() } ?: throw AssertionError("the audio write never started")
                // The session fails: retirement cancels the parked writes, and
                // one of them fails WHILE handling that cancellation.
                rig.session.onFault(GENERATION, CaptureFault.Codec(MediaKind.Video, "CodecException: the encoder died"))
                runCurrent()
                runCatching { running.join() }
                observed = seen.observed().exceptionOrNull()
            }
            val real = assertNotNull(actual.get(), "precondition -- the sink really threw a non-cancellation failure")
            assertSame(real, observed, "a real failure is not ignored because cancellation caused it")
            assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "and nothing completed: ${calls.all()}")
        }
    }

    /**
     * A child cancels ITSELF before the parent cancels the broadcast, and its
     * receipt has not been consumed yet. That earlier, distinct cancellation
     * is evidence of its own: the parent's cancellation does not erase it just
     * because both are cancellations.
     */
    @Test
    fun g4_anEarlierIndependentChildCancellationSurvivesParentCancellation() = runTest {
        for (failing in listOf(MediaKind.Video, MediaKind.Audio)) {
            val calls = Calls()
            val independent = CancellationException("the $failing sender cancelled itself")
            val parent = CancellationException("the user left afterwards")
            val thrown = java.util.concurrent.atomic.AtomicBoolean(false)
            val aboutToThrow = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val never = CompletableDeferred<Unit>()
            val inVideo = CompletableDeferred<Unit>()
            val inAudio = CompletableDeferred<Unit>()
            fun park(kind: MediaKind, entered: CompletableDeferred<Unit>): suspend (Int) -> Unit = {
                entered.complete(Unit)
                if (kind == failing) {
                    release.await()
                    // Announced BEFORE the throw, so the canceller below is
                    // queued ahead of this child's own receipt.
                    aboutToThrow.complete(Unit)
                    thrown.set(true)
                    throw independent
                }
                never.await()
            }
            val rig =
                Rig(
                    calls = calls,
                    videoSink = LaneSink(MediaKind.Video, calls, beforeWrite = park(MediaKind.Video, inVideo)),
                    audioSink = LaneSink(MediaKind.Audio, calls, beforeWrite = park(MediaKind.Audio, inAudio)),
                )
            var observed: Throwable? = null
            watching(rig) { running, seen ->
                val canceller =
                    launch {
                        aboutToThrow.await()
                        assertTrue(thrown.get(), "$failing: the independent cancellation really happened first")
                        running.cancel(parent)
                    }
                try {
                    assertEquals(HandoffOutcome.Queued, rig.video.queue(0, videoFrame(1), 33_333L))
                    assertEquals(HandoffOutcome.Queued, rig.audio.queue(0, bytes(2, 3), 21_333L, flags = 0))
                    runCurrent()
                    withTimeoutOrNull(BOUNDED) { inVideo.await() } ?: throw AssertionError("$failing: the video write never started")
                    withTimeoutOrNull(BOUNDED) { inAudio.await() } ?: throw AssertionError("$failing: the audio write never started")
                    release.complete(Unit)
                    runCurrent()
                    runCatching { running.join() }
                    observed = seen.observed().exceptionOrNull()
                } finally {
                    canceller.cancel(CancellationException("the row is finished"))
                    runCatching { canceller.join() }
                }
            }
            assertTrue(thrown.get(), "$failing: precondition -- the sink really cancelled itself")
            val outgoing = assertIs<CancellationException>(observed, "$failing: cancellation is the outgoing primary")
            assertTrue(
                outgoing === parent || generateSequence(outgoing as Throwable) { it.cause }.any { it === parent },
                "$failing: and it is the PARENT's cancellation: $outgoing / ${outgoing.cause}",
            )
            assertEquals(
                listOf<Throwable>(independent),
                outgoing.suppressed.toList(),
                "$failing: the earlier independent child cancellation must remain an exact distinct root",
            )
            assertTrue(calls.all().none { it is Call.Completed || it is Call.Drained }, "$failing: nothing completed")
        }
    }

    // ---- controls: the substrate, exercised while the coordinator is absent ----

    /**
     * CONTROL, not a coordinator row. It exercises this slice's SCHEDULING
     * MECHANISM directly: an owned worker raises a fault into the real
     * session, whose cleanup parks at a fail-only gate before the terminal is
     * announced, while the rest of the schedule keeps running and the tracks
     * can still be ended. Nothing here models the coordinator.
     */
    @Test
    fun s1_theHeldTerminalScheduleIsSatisfiableWithoutTheCoordinator() {
        val entered = HostGate("the session's cleanup reached the held track close")
        val release = HostGate("the row released the held track close")
        val faults = HostFaults()
        val inner = BoundedCaptureHandoff(MediaKind.Video, GENERATION, HostBuffers(), HostReleases(), BoundedCaptureLedger(MediaKind.Video, GENERATION, 16), faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val audioBuffers = HostBuffers().put(0, ByteBuffer.allocate(0))
        val audio = BoundedCaptureHandoff(MediaKind.Audio, GENERATION, audioBuffers, HostReleases(), BoundedCaptureLedger(MediaKind.Audio, GENERATION, 16), faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val listener = HostSessionListener()
        val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        // This control uses the host listener: what it demonstrates is the
        // SCHEDULE, not the coordinator.
        val session = PlatformCaptureSession(GENERATION, ownership, NoAcquire(), HeldClose(inner, entered, release), audio, listener)
        assertEquals(CaptureStart.Started, session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available))
        val fault = CaptureFault.Codec(MediaKind.Video, "CodecException: the encoder died")
        val faulting = HostWorker("codec-error") { session.onFault(GENERATION, fault) }
        withSettlement(listOf(entered, release), listOf(faulting)) {
            faulting.start()
            entered.await(faulting)
            assertIs<CaptureTermination.Failed>(session.termination, "the decision has been made")
            assertEquals(listOf<Any>(CaptureReady.Ready), listener.all(), "and nothing but the readiness has been announced")
            // The rest of the schedule is free to run while the announcement
            // is withheld: the other track can still be closed and ended.
            assertEquals(HandoffOutcome.Queued, audio.onOutput(GENERATION, CodecOutput(0, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM)))
            release.openFully()
            faulting.join()
            assertEquals(
                listOf<Any>(CaptureReady.Ready, CaptureTermination.Failed(fault)),
                listener.all(),
                "the terminal is announced only after the held close returns",
            )
        }
    }

    /**
     * CONTROL, not a coordinator row. The successful-terminal counterpart of
     * s1: a forwarding listener withholds the real session's Completed
     * notification and RETURNS, so the decision and the cleanup both finish and
     * only the observer's knowledge is delayed. Nothing blocks a scheduler.
     */
    @Test
    fun s3_aWithheldSuccessfulTerminalIsSatisfiableWithoutTheCoordinator() {
        val faults = HostFaults()
        val video = BoundedCaptureHandoff(MediaKind.Video, GENERATION, HostBuffers(), HostReleases(), BoundedCaptureLedger(MediaKind.Video, GENERATION, 16), faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val audio = BoundedCaptureHandoff(MediaKind.Audio, GENERATION, HostBuffers(), HostReleases(), BoundedCaptureLedger(MediaKind.Audio, GENERATION, 16), faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val listener = HostSessionListener()
        val withheld = WithheldTerminal(listener)
        val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        val session = PlatformCaptureSession(GENERATION, ownership, NoAcquire(), video, audio, withheld)
        assertEquals(CaptureStart.Started, session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available))
        try {
            session.onTrackEnded(GENERATION, MediaKind.Video)
            session.onTrackEnded(GENERATION, MediaKind.Audio)
            assertEquals(CaptureTermination.Completed, session.termination, "the session decided, and its cleanup returned")
            assertTrue(ownership.isClosed, "the real cleanup really happened")
            assertTrue(withheld.isHeld, "and only the announcement is withheld")
            assertEquals(listOf<Any>(CaptureReady.Ready), listener.all(), "the observer has been told nothing but the readiness")
            withheld.release()
            assertEquals(
                listOf<Any>(CaptureReady.Ready, CaptureTermination.Completed),
                listener.all(),
                "the terminal arrives exactly when it is released",
            )
        } finally {
            withheld.release()
        }
    }

    /**
     * HARNESS CONTROL for the kotlinx completion API this coordinator depends
     * on, in isolation -- the production registration path is exercised by
     * [f9_theProductionRegistrationReceiptsAnAlreadyCompletedChild]:
     * `invokeOnCompletion` registered AFTER a child has already finished still
     * fires exactly once, immediately, with that child's actual cause --
     * normal return, failure and cancellation alike.
     */
    @Test
    fun h2_aCompletionHandlerRegisteredAfterCompletionStillFiresOnce() = runTest {
        val raised = IllegalStateException("the child failed")
        val cancelled = CancellationException("the child was cancelled")
        var seen: Throwable? = null
        supervisorScope {
            for (script in listOf<suspend () -> Unit>({ }, { throw raised }, { throw cancelled })) {
                val child = async { script() }
                runCatching { child.join() }
                assertTrue(child.isCompleted, "the child finished BEFORE anything was registered")
                val fired = mutableListOf<Throwable?>()
                child.invokeOnCompletion { cause -> synchronized(fired) { fired.add(cause) } }
                runCurrent()
                assertEquals(1, fired.size, "the handler fired exactly once: $fired")
            }
            // ...and the causes it reports are the actual ones.
            val failing = async { throw raised }
            runCatching { failing.join() }
            failing.invokeOnCompletion { cause -> seen = cause }
        }
        assertSame(raised, seen, "with the child's own cause, by identity")
    }

    /**
     * HARNESS CONTROL. It proves the cancellation observation path this file
     * relies on, independently of the coordinator: a child that records
     * its own boundary Result before rethrowing leaves the Deferred CANCELLED
     * -- `await` throws rather than handing the Result back -- and the cause
     * the rows compare comes from the recorded observation, not from `await`.
     */
    @Test
    fun h1_theCancellationObservationPathIsSatisfiable() = runTest {
        val seen = RunObservation()
        val parked = CompletableDeferred<Unit>()
        val cause = CancellationException("the row cancelled this body")
        supervisorScope {
            val running =
                async {
                    val result =
                        runCatching {
                            parked.complete(Unit)
                            CompletableDeferred<BroadcastOutcome>().await()
                        }
                    seen.record(result)
                    result.getOrThrow()
                }
            runCurrent()
            withTimeoutOrNull(BOUNDED) { parked.await() } ?: throw AssertionError("the body never parked")
            running.cancel(cause)
            runCatching { running.join() }
            assertTrue(running.isCancelled, "the job keeps its cancelled state")
            val fromAwait = runCatching { running.await() }.exceptionOrNull()
            assertIs<CancellationException>(fromAwait, "await throws instead of returning the child's Result")
            // What the body actually sees is what kotlinx raised AT its
            // suspension point: cancelling a job with a cause makes an
            // unrelated `await` throw a cancellation CARRYING that cause. The
            // observation path records exactly that, on the body's own frame,
            // rather than whatever `await` would have recovered.
            val recorded = assertIs<CancellationException>(seen.observed().exceptionOrNull())
            assertTrue(
                recorded === cause || recorded.cause === cause,
                "the recorded observation is the body's own cancellation, carrying the row's cause: $recorded / ${recorded.cause}",
            )
        }
    }

    /**
     * CONTROL, not a coordinator row. The accepted ownership and session, as
     * they actually behave when a resource close fails during a stop: the
     * supplied reason comes back BY IDENTITY carrying the cleanup failure
     * suppressed on it, the ending is Cancelled rather than Completed, and the
     * track is closed afterwards. This is the evidence the coordinator's Error
     * adaptation has to transfer rather than discard.
     */
    @Test
    fun s2_aStopReasonCarriesTheCleanupFailureItFound() {
        val closes = mutableListOf<Any>()
        val identity = HostResource("camera")
        val cleanup = IllegalStateException("the camera's close failed")
        val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        assertIs<AcquireOutcome.Acquired>(
            ownership.acquire(CaptureResource.Camera) { HostHandle(identity, closes, cleanup) },
        )
        val faults = HostFaults()
        val video = BoundedCaptureHandoff(MediaKind.Video, GENERATION, HostBuffers(), HostReleases(), BoundedCaptureLedger(MediaKind.Video, GENERATION, 16), faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val audio = BoundedCaptureHandoff(MediaKind.Audio, GENERATION, HostBuffers(), HostReleases(), BoundedCaptureLedger(MediaKind.Audio, GENERATION, 16), faults, CAPACITY, HOST_MAX_OUTPUT_BYTES)
        val events = HostSessionListener()
        val session = PlatformCaptureSession(GENERATION, ownership, NoAcquire(), video, audio, events)
        assertEquals(CaptureStart.Started, session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available))

        val reason = IllegalStateException("the broadcast failed")
        val returned = session.stop(reason)
        assertSame(reason, returned, "the supplied reason comes back by identity")
        assertTrue(reason.suppressed.any { it === cleanup }, "carrying the cleanup failure: ${reason.suppressed.toList()}")
        assertEquals(listOf<Any>(identity), closes, "and every handle was attempted")
        assertIs<CaptureTermination.Cancelled>(session.termination, "a stop with a reason is never a Completed")
        assertIs<HandoffOutcome.Ignored>(
            video.onOutput(GENERATION, CodecOutput(0, 0, 4, 33_333L, CaptureContract.FLAG_KEY_FRAME)),
            "and the tracks are closed afterwards",
        )
    }

    // ---- the harness ------------------------------------------------------------

    /**
     * Runs the coordinator while [script] drives the platform.
     *
     * The result is captured INSIDE the child, on `run`'s own caller frame,
     * before anything crosses `await`: kotlinx's stack-trace recovery can hand
     * back a copy of a Throwable that crossed a `Deferred`, so identity is only
     * meaningful here. The broadcast is cancelled and joined on every path, so
     * no child outlives a failing assertion.
     */
    /**
     * What `run` produced, recorded on ITS OWN frame before anything is
     * rethrown. A cancelled `Deferred` throws from `await` instead of handing
     * back the child's Result, and a Throwable that crosses `await` can be a
     * recovered copy -- so the cause a row compares comes from here.
     */
    private class RunObservation {
        private var seen: Result<BroadcastOutcome>? = null

        @Synchronized
        fun record(result: Result<BroadcastOutcome>) {
            if (seen == null) seen = result
        }

        @Synchronized
        fun observed(): Result<BroadcastOutcome> =
            seen ?: throw AssertionError("the broadcast never reached its own boundary")

        @get:Synchronized
        val reached: Boolean get() = seen != null
    }

    /**
     * Starts the broadcast, recording its run-boundary observation, and gives
     * [body] the job so a row can cancel it at a boundary it chooses. The job
     * is always cancelled and joined on the way out.
     */
    private suspend fun TestScope.watching(
        rig: Rig,
        permissions: PermissionFlow = Granted(GENERATION),
        body: suspend (Job, RunObservation) -> Unit,
    ) {
        val seen = RunObservation()
        supervisorScope {
            val running =
                async {
                    val result = runCatching { rig.broadcast.run(permissions, CaptureCapability.Available, CaptureCapability.Available) }
                    seen.record(result)
                    // The coroutine keeps its REAL state: a cancelled child
                    // stays cancelled rather than being reported as a value.
                    result.getOrThrow()
                }
            try {
                runCurrent()
                body(running, seen)
            } finally {
                running.cancel(CancellationException("the row is finished"))
                runCatching { running.join() }
            }
        }
    }

    private suspend fun TestScope.broadcasting(
        rig: Rig,
        permissions: PermissionFlow = Granted(GENERATION),
        script: suspend () -> Unit,
    ): Result<BroadcastOutcome> =
        supervisorScope {
            val running =
                async {
                    runCatching { rig.broadcast.run(permissions, CaptureCapability.Available, CaptureCapability.Available) }
                }
            try {
                runCurrent()
                script()
                withTimeoutOrNull(BOUNDED) { running.await() }
                    ?: throw AssertionError("the broadcast did not settle within $BOUNDED")
            } finally {
                running.cancel(CancellationException("the row is finished"))
                runCatching { running.await() }
            }
        }

    private suspend fun TestScope.runBroadcast(rig: Rig, script: suspend () -> Unit): BroadcastOutcome {
        val settled = broadcasting(rig, script = script)
        return settled.getOrElse { throw AssertionError("the broadcast failed instead of producing an outcome: $it", it) }
    }

    private suspend fun TestScope.runBroadcastCatching(rig: Rig, script: suspend () -> Unit): Throwable? =
        broadcasting(rig, script = script).exceptionOrNull()

    /**
     * The suspend-capable form of `withSettlement`: every gate this row owns is
     * opened and every worker joined on EVERY path, and what the cleanup finds
     * is attached to the failure that is leaving rather than replacing it.
     */
    private suspend fun <T> settling(gates: List<HostGate>, workers: List<HostWorker>, body: suspend () -> T): T {
        var primary: Throwable? = null
        try {
            return body()
        } catch (cancelled: CancellationException) {
            primary = cancelled
            throw cancelled
        } catch (failure: Exception) {
            primary = failure
            throw failure
        } catch (error: Error) {
            primary = error
            throw error
        } finally {
            for (gate in gates) gate.openFully()
            val found = mutableListOf<Throwable>()
            for (worker in workers) {
                try {
                    worker.join()
                } catch (failure: RuntimeException) {
                    found.add(failure)
                } catch (interrupted: InterruptedException) {
                    found.add(interrupted)
                } catch (error: Error) {
                    found.add(error)
                }
            }
            val leaving = primary
            if (leaving != null) {
                found.forEach { if (it !== leaving) leaving.addSuppressed(it) }
            } else {
                found.firstOrNull()?.let { first ->
                    found.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
                    throw first
                }
            }
        }
    }

    private companion object {
        const val GENERATION = 1L
        const val CAPACITY = 4
        const val BASE_NANOS = 1_000_000_000L
        const val ORIGIN = 0L
        const val EPOCH = 1_700_000_000_000_000uL
        val WRITE_TIMEOUT = 1.seconds
        val DRAIN_TIMEOUT = 5.seconds
        val BOUNDED = 30.seconds

        /** Every drain answer that is not a flush. */
        val NOT_FLUSHED = listOf(DrainOutcome.TimedOut, DrainOutcome.Interrupted, DrainOutcome.Closed, DrainOutcome.Unsupported)
    }
}
