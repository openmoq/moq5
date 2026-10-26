package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
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
 * The composition root of one live broadcast, measured end to end through the
 * ACCEPTED pieces: the real capture session, the real ownership and handoffs,
 * the accepted pipeline, per-track drains, the accepted coordinator and the
 * session-owned discard record.
 *
 * Only the genuine external boundaries are scripted: the acquisition driver
 * and the resources it installs, the codec buffers and index ownership, the
 * callback dispatcher, and the two sender-facing sinks with the completion
 * half. Nothing here re-implements a coordinator, a sender or a success
 * counter.
 *
 * The rows named `c` measure `composeLiveBroadcast`. The rows named `m`
 * measure this file's own machinery -- its settlement, its buffer gate and its
 * ownership -- against the accepted components, so a row that passes is known
 * to be measuring something.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class LiveCompositionRedTest {

    private sealed interface Call {
        data class Wrote(val kind: MediaKind, val obj: SendObject) : Call

        data class Ended(val kind: MediaKind) : Call

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

    private class LaneSink(private val kind: MediaKind, private val calls: Calls) : TrackSink {
        override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome {
            calls.add(Call.Wrote(kind, obj))
            return WriteOutcome.Accepted
        }

        override suspend fun endTrack(timeout: Duration): WriteOutcome {
            calls.add(Call.Ended(kind))
            return WriteOutcome.Accepted
        }
    }

    private class Completion(private val calls: Calls) : CompletionSink {
        override suspend fun complete() = calls.add(Call.Completed)

        override suspend fun drain(timeout: Duration): DrainOutcome {
            calls.add(Call.Drained(timeout))
            return DrainOutcome.Flushed
        }
    }

    /**
     * The platform's acquisition driver, scripted and COUNTED: how many times
     * it was entered, and how many resources it actually tried to acquire.
     *
     * What it installs are the row's OWN resources -- the very codecs the
     * composition hands to the handoffs -- so closing what this acquired is
     * what disposes them, and a wiring that leases something else can be seen
     * from the disposal inventory.
     */
    private class CountingAcquirer(
        override val order: List<CaptureResource>,
        private val bind: (CaptureOwnership) -> Unit,
        private val open: (CaptureResource) -> ResourceHandle,
    ) : CaptureAcquirer {
        private val entries = java.util.concurrent.atomic.AtomicInteger(0)
        private val asked = java.util.concurrent.atomic.AtomicInteger(0)

        fun begins(): Int = entries.get()

        fun acquisitions(): Int = asked.get()

        /** What the platform does inside `begin`, BEFORE the first resource is opened. */
        var duringBegin: () -> Unit = {}

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
            entries.incrementAndGet()
            // The codecs learn their worker retention HERE: the driver has the
            // ownership and no callback can have been admitted yet.
            bind(ownership)
            duringBegin()
            for (resource in order) {
                asked.incrementAndGet()
                val outcome = ownership.acquire(resource) { open(resource) }
                if (outcome is AcquireOutcome.Failed) return outcome
            }
            return null
        }
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

    /**
     * The codec's retention on the callback worker, bound ONCE to the
     * ownership the composition actually builds.
     *
     * Assembly stays passive -- the codecs are constructed before any
     * ownership exists -- and the codec's lifetime identity stays the single
     * stable one, because only this delegate's target changes. A retention
     * asked for before the binding, or a binding to a second ownership, is a
     * wiring mistake and is refused rather than silently granted.
     */
    private class LateWorker : WorkerRetention {
        private var bound: WorkerRetention? = null

        @Synchronized
        fun bind(ownership: WorkerRetention) {
            val already = bound
            require(already == null || already === ownership) {
                "the worker retention is already bound to another ownership"
            }
            bound = ownership
        }

        @Synchronized
        private fun target(): WorkerRetention = checkNotNull(bound) { "no ownership is bound to this worker retention yet" }

        override fun retainWorker(): Boolean = target().retainWorker()

        override fun releaseWorker() = target().releaseWorker()
    }

    /**
     * The codec's buffer access, held open at ONE index.
     *
     * This is the real transfer boundary: the accepted handoff fetches the
     * buffer only after its codec lease has been granted, so a callback parked
     * here is a callback holding a lease inside the codec -- no composition
     * test hook required.
     */
    private class GatedBuffers(
        private val inner: HostBuffers,
        private val at: Int,
        private val reached: HostGate,
        private val release: HostGate,
    ) : OutputBufferAccess {
        override fun outputBuffer(index: Int): ByteBuffer? {
            if (index == at) {
                reached.open()
                release.await()
            }
            return inner.outputBuffer(index)
        }
    }

    /** What the row hands the composition root, and what it keeps to drive the platform. */
    private class Rig(
        order: List<CaptureResource> = FULL_ORDER,
        /** When named, the video transfer at that index is held open until the row lets it go. */
        videoHold: Triple<Int, HostGate, HostGate>? = null,
        /** The same for the audio transfer. */
        audioHold: Triple<Int, HostGate, HostGate>? = null,
        /** False builds the wiring that FORGETS the worker retention, so its absence can be seen. */
        retainWorker: Boolean = true,
        /** When named, opening THAT resource fails, leaving the ones before it installed. */
        private val failAt: CaptureResource? = null,
        val record: CaptureDiscardRecord = CaptureDiscardRecord(GENERATION),
    ) {
        val calls = Calls()
        val cleanup = mutableListOf<String>()
        val others = mutableListOf<Any>()
        val videoBuffers = HostBuffers()
        val videoIndices = HostReleases()
        val audioBuffers = HostBuffers()
        val audioIndices = HostReleases()
        val worker = LateWorker()

        /** ONE lifetime per codec, shared by that track's handoff and the handle that closes it. */
        val videoCodec = HostCodec(cleanup, name = "video-codec", worker = worker.takeIf { retainWorker })
        val audioCodec = HostCodec(cleanup, name = "audio-codec", worker = worker.takeIf { retainWorker })
        val dispatcher = HostDispatcher()

        /** The same dispatcher, observed: every delivery attempt through the composition is counted. */
        val deliveries = CountingDispatcher(dispatcher)
        val keying = HostAudioKeying(SAMPLE_RATE, FRAMES_PER_UNIT, ORIGIN)
        val acquirer =
            CountingAcquirer(order, bind = { worker.bind(it) }) { resource ->
                if (resource == failAt) throw IllegalStateException("$resource could not be opened")
                when (resource) {
                    CaptureResource.Worker -> HostWorkerHandle(dispatcher, cleanup, "worker")
                    CaptureResource.VideoEncoder -> HostCodecHandle(videoCodec, "video-codec", cleanup)
                    CaptureResource.AudioEncoder -> HostCodecHandle(audioCodec, "audio-codec", cleanup)
                    else -> HostHandle(HostResource(resource.name.lowercase()), others)
                }
            }
        val videoAccess: OutputBufferAccess =
            videoHold?.let { (at, reached, release) -> GatedBuffers(videoBuffers, at, reached, release) } ?: videoBuffers
        val audioAccess: OutputBufferAccess =
            audioHold?.let { (at, reached, release) -> GatedBuffers(audioBuffers, at, reached, release) } ?: audioBuffers
        val environment =
            CaptureEnvironment(
                acquirer = acquirer,
                dispatcher = deliveries,
                late = LateLog(),
                video = TrackCodec(videoCodec, videoAccess, videoIndices),
                audio = TrackCodec(audioCodec, audioAccess, audioIndices),
            )
        val sinks = PublishSinks(LaneSink(MediaKind.Video, calls), LaneSink(MediaKind.Audio, calls), Completion(calls))

        /** Every close, disposal and dispatcher retirement that happened, in order. */
        fun cleanupNow(): List<String> = synchronized(cleanup) { cleanup.toList() }

        /** Every disposal that actually happened, by codec, in order. */
        fun disposals(): List<String> = cleanupNow().filter { it.startsWith("dispose:") }
    }

    private fun bytes(tag: Int, size: Int) = ByteArray(size) { ((tag shl 4) + it).toByte() }

    private fun compose(
        rig: Rig,
        permissions: PermissionFlow = Granted(GENERATION),
        keying: AudioKeying = rig.keying,
        maxOutputBytes: Int = HOST_MAX_OUTPUT_BYTES,
        audio: CompleteTrackFormat = validatedAudio(),
    ): LiveCapture =
        composeLiveBroadcast(
            generation = GENERATION,
            permissions = permissions,
            cameraFacts = CaptureCapability.Available,
            audioFacts = CaptureCapability.Available,
            calibration = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L),
            origin = ORIGIN,
            audioKeying = keying,
            capacity = CAPACITY,
            maxOutputBytes = maxOutputBytes,
            maxNalUnits = HOST_MAX_NAL_UNITS,
            video = validatedVideo(),
            audio = audio,
            environment = rig.environment,
            sinks = rig.sinks,
            record = rig.record,
            writeTimeout = WRITE_TIMEOUT,
            drainTimeout = DRAIN_TIMEOUT,
        )

    /**
     * One encoder callback delivered the way a platform driver must: the
     * completed access unit is OBSERVED on that track's own ledger under its
     * own key, and only then is the codec callback delivered to its handoff.
     * Nothing here derives a capture instant or a flag on the driver's behalf.
     */
    private fun deliver(
        inputs: TrackInputs,
        buffers: HostBuffers,
        index: Int,
        payload: ByteArray,
        pts: Long,
        captureNanos: Long,
    ): HandoffOutcome {
        buffers.put(index, ByteBuffer.wrap(payload))
        assertEquals(
            ObserveOutcome.Recorded,
            inputs.ledger.observe(inputs.generation, inputs.kind, CaptureObservation(pts, captureNanos)),
            "the driver observes its own completed access unit",
        )
        return inputs.handoff.onOutput(inputs.generation, CodecOutput(index, 0, payload.size, pts, CaptureContract.FLAG_KEY_FRAME))
    }

    /** The encoder's own terminal, delivered explicitly -- nothing synthesises one. */
    private fun deliverEnd(inputs: TrackInputs, buffers: HostBuffers, index: Int): HandoffOutcome {
        buffers.put(index, ByteBuffer.allocate(0))
        return inputs.handoff.onOutput(inputs.generation, CodecOutput(index, 0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM))
    }

    // ---- the rows --------------------------------------------------------------

    /**
     * A complete broadcast through the composed pieces: each track's own
     * driver observes and delivers its media and then its OWN end-of-stream,
     * and the broadcast publishes, ends, completes and drains exactly once.
     *
     * The audio timestamps are not written down here: the row feeds PCM reads
     * to the keying the composition handed back and publishes the units that
     * route says became complete, so a replacement keying object or a
     * hardcoded geometry cannot satisfy it.
     *
     * The cross-track order is DECLARED by this row -- video's media, then
     * audio's two units, then video's terminal, then audio's -- not left to
     * whichever coroutine happens to run.
     */
    /**
     * The composition binds the converted payload to the SAME numeric cap it
     * gives its handoffs: a 32-byte output the handoff admits, whose avc1
     * payload needs 36 bytes -- an IDR, then four slices after three-byte
     * prefixes, each growing by one -- is refused, and nothing is written.
     */
    @Test
    fun c1b_theComposedLaneBoundsTheConvertedPayloadByTheHandoffCap() = runTest {
        val rig = Rig()
        val frame = byteArrayOf(0, 0, 0, 1, 0x65, 0x88.toByte(), 0x84.toByte(), 0x21) +
            List(4) { byteArrayOf(0, 0, 1, 0x41, 0x9a.toByte(), 0x02) }.reduce { a, b -> a + b }
        assertEquals(32, frame.size, "setup: 4+4 + 4 x (3+3)")
        val live = compose(rig, maxOutputBytes = frame.size)
        val settled =
            withRun(live.run) { owner ->
                runCurrent()
                assertEquals(HandoffOutcome.Queued, deliver(live.video, rig.videoBuffers, 0, frame, VIDEO_PTS, VIDEO_CAPTURE))
                runCurrent()
                withTimeoutOrNull(BOUNDED) { owner.job.join() } ?: throw AssertionError("the broadcast did not settle")
                owner.observed()
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(settled.getOrThrow())
        assertEquals("the Video payload was refused: the converted payload needs 36 bytes; the limit is 32", unfinished.reason)
        assertTrue(rig.calls.all().none { it is Call.Wrote }, "nothing was written: ${rig.calls.all()}")
    }

    @Test
    fun c1_bothTracksPublishThroughTheComposedPiecesAndCompleteOnce() = runTest {
        val rig = Rig()
        val live = compose(rig)
        val video = VIDEO_ANNEXB.copyOf()
        val first = bytes(2, 3)
        val second = bytes(3, 4)

        // The keying route, exercised on its own terms and compared against
        // geometry this row declares.
        val keying = assertNotNull(live.audio.keying, "the audio track is keyed")
        assertSame(rig.keying, keying, "and it is the keying that was supplied, not a replacement")
        assertEquals(0, keying.retainedFrames, "assembly read nothing: the driver's first read is the first")
        assertEquals(emptyList(), keying.onRead(600), "600 frames complete no unit of $FRAMES_PER_UNIT")
        assertEquals(600, keying.retainedFrames)
        assertEquals(listOf(0L), keying.onRead(600), "the next 600 complete unit 0")
        assertEquals(176, keying.retainedFrames, "and the remainder is retained, not dropped")
        assertEquals(listOf(1L), keying.onRead(900), "900 more complete unit 1")
        assertEquals(52, keying.retainedFrames)
        val unitZero = assertIs<AudioKeyOutcome.Key>(keying.keyFor(0L)).presentationTimeUs
        val unitOne = assertIs<AudioKeyOutcome.Key>(keying.keyFor(1L)).presentationTimeUs
        assertEquals(ORIGIN, unitZero, "unit 0 sits on the broadcast's own origin")
        assertEquals(ORIGIN + 21_333L, unitOne, "and unit 1 one unit later: 1024 frames at 48 kHz, floored once")

        val settled =
            withRun(live.run) { owner ->
                runCurrent()
                assertEquals(HandoffOutcome.Queued, deliver(live.video, rig.videoBuffers, 0, video, VIDEO_PTS, VIDEO_CAPTURE))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, deliver(live.audio, rig.audioBuffers, 0, first, unitZero, FIRST_CAPTURE))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, deliver(live.audio, rig.audioBuffers, 1, second, unitOne, SECOND_CAPTURE))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, deliverEnd(live.video, rig.videoBuffers, 1))
                runCurrent()
                assertEquals(HandoffOutcome.Queued, deliverEnd(live.audio, rig.audioBuffers, 2))
                runCurrent()
                withTimeoutOrNull(BOUNDED) { owner.job.join() } ?: throw AssertionError("the broadcast did not settle")
                owner.observed()
            }
        assertIs<BroadcastOutcome.Published>(settled.getOrThrow())

        val calls = rig.calls.all()
        val writes = calls.filterIsInstance<Call.Wrote>()
        assertEquals(
            listOf(MediaKind.Video, MediaKind.Audio, MediaKind.Audio),
            writes.map { it.kind },
            "each lane wrote through its OWN sink, in the order this row declared",
        )
        assertContentEquals(VIDEO_AVC1, writes[0].obj.payload.bytes(), "with the exact captured bytes, converted")
        assertContentEquals(first, writes[1].obj.payload.bytes())
        assertContentEquals(second, writes[2].obj.payload.bytes())
        assertEquals(MediaMicros(33_333uL), writes[0].obj.presentationTime, "video on its own timeline over the shared origin")
        assertEquals(MediaMicros(0uL), writes[1].obj.presentationTime, "and audio on its own, unaffected by the video output")
        assertEquals(MediaMicros(21_333uL), writes[2].obj.presentationTime, "one keyed unit apart")
        assertEquals(UnixMicros(EPOCH + 40_000uL), writes[0].obj.captureTime, "the instant that track's ledger claimed")
        assertEquals(UnixMicros(EPOCH + 38_000uL), writes[1].obj.captureTime, "which is not its presentation time")
        assertEquals(UnixMicros(EPOCH + 60_000uL), writes[2].obj.captureTime)
        assertTrue(writes[0].obj.isSync && writes[0].obj.startsGroup, "a video key frame opens its group")
        assertTrue(writes[1].obj.isSync && !writes[1].obj.startsGroup, "an audio access unit is independent and opens none")
        assertTrue(writes[2].obj.isSync && !writes[2].obj.startsGroup)
        // The rest of the published object, declared rather than adopted: this
        // sample states no group end, no separate decode order, no SAP type
        // and no CMAF properties.
        for (write in writes) {
            assertFalse(write.obj.endsGroup, "${write.kind} closes no group here")
            assertNull(write.obj.decodeTime, "and declares no decode time distinct from its presentation time")
            assertEquals(write.obj.presentationTime, write.obj.decodeTimeOrPresentation())
            assertNull(write.obj.sapType, "no SAP type is declared")
            assertNull(write.obj.cmafProperties, "and no CMAF properties are attached")
        }
        assertEquals(
            listOf<Call>(Call.Ended(MediaKind.Video), Call.Ended(MediaKind.Audio), Call.Drained(DRAIN_TIMEOUT), Call.Completed, Call.Drained(DRAIN_TIMEOUT)),
            calls.drop(writes.size),
            "both ends, then the media drain, exactly one completion, and the catalog drain",
        )
        assertEquals(listOf(0, 1), rig.videoIndices.indices(), "every video index went back to its codec, once")
        assertEquals(listOf(0, 1, 2), rig.audioIndices.indices())
        assertEquals(emptyMap(), rig.record.snapshot(), "nothing was discarded, so nothing is observed")
        assertEquals(0, rig.videoCodec.outstandingLeases, "every lease was given back")
        assertEquals(0, rig.audioCodec.outstandingLeases)
        assertEquals(
            listOf("dispose:audio-codec", "dispose:video-codec"),
            rig.disposals(),
            "and both codecs the acquirer installed were disposed, once each",
        )
    }

    /** A refused permission acquires nothing: the driver is never even entered. */
    @Test
    fun c2_aRefusedPermissionAcquiresNothingAndWritesNothing() = runTest {
        val refusing = Rig()
        val outcome = compose(refusing, Refused(GENERATION, "camera permanently denied")).run()
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome, "a refused start is a named outcome")
        assertEquals(
            "the capture session refused to start: camera permanently denied",
            unfinished.reason,
            "named by the refusal it actually got",
        )
        assertEquals(0, refusing.acquirer.begins(), "the driver was never entered")
        assertEquals(0, refusing.acquirer.acquisitions(), "and nothing was acquired")
        assertEquals(emptyList(), refusing.calls.all(), "the sender was never asked for anything")
        assertEquals(emptyList(), refusing.disposals(), "nothing was acquired, so nothing was disposed")
        assertFalse(refusing.videoCodec.everLeased, "and no transfer ever leased a codec")

        // The CONTROL: the same counters, on a start that really is allowed,
        // acquiring the very codecs this rig hands to the composition.
        val allowed = Rig()
        val live = compose(allowed)
        withRun(live.run) { owner ->
            runCurrent()
            assertEquals(1, allowed.acquirer.begins(), "the counter really is connected: this start entered the driver")
            assertEquals(FULL_ORDER.size, allowed.acquirer.acquisitions(), "and acquired every resource in its order")
            assertFalse(owner.job.isCompleted, "and the run is still LIVE here, not already returned")
            val cause = owner.cancel("the control has seen what it needed")
            withTimeoutOrNull(BOUNDED) { owner.job.join() } ?: throw AssertionError("the control's run did not settle")
            val raised = assertNotNull(owner.observed().exceptionOrNull(), "a cancelled run produces no outcome")
            assertIs<CancellationException>(raised)
            assertTrue(recovers(raised, cause), "it left as the cancellation this control issued: $raised")
            assertEquals(emptyList(), raised.suppressed.toList(), "carrying no cleanup failure of its own")
        }
        assertEquals(
            listOf("dispose:audio-codec", "dispose:video-codec"),
            allowed.disposals(),
            "and the cancelled control disposed what it acquired, once each",
        )
    }

    /**
     * Cancellation with a REAL admitted callback still inside the codec: the
     * broadcast is cancelled while that transfer holds its lease, the
     * cancellation is serviced with the callback STILL held, and what the
     * failed close then throws away is kept as the partial fact it is.
     */
    @Test
    fun c3_cancellationStopsCaptureAndKeepsThePartialRecord() = runTest {
        val reached = HostGate("the video transfer reached the codec buffer")
        val release = HostGate("the row released the held transfer")
        val rig = Rig(videoHold = Triple(0, reached, release))
        val live = compose(rig)
        val worker = HostWorker("transfer") { deliver(live.video, rig.videoBuffers, 0, bytes(1, 6), VIDEO_PTS, VIDEO_CAPTURE) }
        val settled =
            settling(listOf(reached, release), listOf(worker)) {
                withRun(live.run) { owner ->
                    runCurrent()
                    worker.start()
                    reached.await(worker)
                    // A real admitted output is inside the codec RIGHT NOW,
                    // under a lease it was granted before the fetch.
                    assertEquals(1, rig.videoCodec.outstandingLeases, "the held transfer really holds a lease")
                    assertFalse(rig.videoCodec.isDisposed)

                    owner.cancel("the user left")
                    runCurrent()
                    // Only the LEASED codec is deferred. Audio is not leased,
                    // so the ordered close disposes it where it stands, and
                    // the worker the held callback runs on is retained.
                    assertEquals(
                        listOf("close:audio-codec", "dispose:audio-codec", "close:video-codec"),
                        rig.cleanupNow(),
                        "the video handle's close really ran, and asked for a disposal a lease defers",
                    )
                    assertEquals(1, rig.videoCodec.outstandingLeases, "the lease outlives the close that asked")
                    assertFalse(rig.videoCodec.isDisposed, "so the codec is not destroyed under a callback inside it")
                    assertEquals(1, rig.audioCodec.disposalCount, "while the codec nothing holds is disposed, once")
                    assertTrue(rig.dispatcher.isAlive, "and the worker that callback is delivering on is still alive")

                    release.openFully()
                    worker.join()
                    runCurrent()
                    assertEquals(0, rig.videoCodec.outstandingLeases, "the last lease returned")
                    assertTrue(rig.videoCodec.isDisposed, "and only then was the codec destroyed")
                    assertEquals(1, rig.videoCodec.disposalCount, "exactly once")
                    assertEquals(
                        listOf(
                            "close:audio-codec",
                            "dispose:audio-codec",
                            "close:video-codec",
                            "dispose:video-codec",
                            "close:worker",
                            "quit:dispatcher",
                        ),
                        rig.cleanupNow(),
                        "each codec disposed once, and the worker retired after the codec that ran on it",
                    )
                    assertFalse(rig.dispatcher.isAlive)
                    assertEquals(listOf(0), rig.videoIndices.indices(), "the held index went back to its codec")
                    assertEquals(emptyList(), rig.audioIndices.indices(), "and audio, which never ran, returned none")
                    owner.observed()
                }
            }
        val raised = assertNotNull(settled.exceptionOrNull(), "a cancelled broadcast produces no outcome: $settled")
        assertIs<CancellationException>(raised, "it leaves as the cancellation it was, not as a failure")
        assertEquals(
            emptyList(),
            raised.suppressed.toList(),
            "and carries no cleanup failure: this cancellation is the carrier, so an empty one is the claim",
        )
        assertTrue(
            rig.calls.all().none { it is Call.Completed || it is Call.Drained },
            "a cancelled broadcast completes nothing: ${rig.calls.all()}",
        )
        assertEquals(
            mapOf(MediaKind.Video to 1),
            rig.record.snapshot(),
            "and the output that failed close threw away is kept as the partial fact it is",
        )
    }

    /**
     * A keying placed on a different origin is refused where it is supplied:
     * audio keyed a fixed distance from its own video is not a broadcast, and
     * an assembly that never compares the two would build one.
     */
    @Test
    fun c4_audioKeyedToAnotherOriginIsRefusedAtAssembly() = runTest {
        val rig = Rig()
        val elsewhere = HostAudioKeying(SAMPLE_RATE, FRAMES_PER_UNIT, ORIGIN + 1L)
        val refused = assertFailsWith<IllegalArgumentException> { compose(rig, keying = elsewhere) }
        assertEquals(
            "the audio keying is keyed to origin ${ORIGIN + 1L}, not the broadcast origin $ORIGIN",
            refused.message,
            "refused by name, at the place the disagreement is visible",
        )
        assertEquals(0, rig.acquirer.begins(), "and nothing was begun on the way to refusing")
    }

    /**
     * A transparent observer over a real dispatcher: it counts each delivery
     * attempt and forwards it, so the dispatcher's own semantics -- inline
     * while alive, refused after quit -- are unchanged.
     */
    private class CountingDispatcher(private val inner: CallbackDispatcher) : CallbackDispatcher {
        @Volatile
        var attempts = 0
            private set

        override val isAlive: Boolean get() = inner.isAlive

        override fun deliver(callback: () -> Unit): Boolean {
            attempts++
            return inner.deliver(callback)
        }

        override fun quit() = inner.quit()
    }

    // ---- k: the audio keying against the validated AAC authority --------------------

    /** The observer counts a real delivery, which still runs its callback exactly once and retires nothing. */
    @Test
    fun k4_theDeliveryObserverCountsARealDeliveryThatRunsOnceAndRetiresNothing() {
        val counting = CountingDispatcher(HostDispatcher())
        var ran = 0
        assertEquals(0, counting.attempts)
        assertTrue(counting.deliver { ran++ })
        assertEquals(1, counting.attempts, "the attempt is counted")
        assertEquals(1, ran, "the callback ran exactly once")
        assertTrue(counting.isAlive, "and nothing was retired")
    }

    private val format48k by lazy { validatedAudio() }
    private val format44k by lazy { validatedAudio(FIXTURE_ASC_44100, 44_100, 2) }

    /** The supported AAC facts the validator retained: the only authority for rate and unit geometry. */
    private fun facts(format: CompleteTrackFormat) = assertIs<FormatAuthority.Aac>(format.authority).facts

    /**
     * A real keying with nontrivial state: unit 0 already completed and 300
     * frames of unit 1 retained. A row proves the state is still exactly that
     * by reading the unit's remaining `framesPerUnit - 300` frames -- 724 for
     * a 1024-frame unit, 660 for 960 -- and completing unit 1.
     */
    private fun keyingWithState(sampleRate: Int, framesPerUnit: Int, originUs: Long): UnitAudioKeying =
        UnitAudioKeying(sampleRate, framesPerUnit, originUs).also {
            assertEquals(listOf(0L), it.onRead(framesPerUnit + 300), "setup: unit 0 completed")
            assertEquals(300, it.retainedFrames, "setup: 300 frames of unit 1 retained")
        }

    /**
     * A refused assembly: named, and none of the recorded effects -- no
     * acquisition begun or resource opened, no close or disposal, no callback
     * delivery attempted and no dispatcher retired, no sender call -- with the
     * supplied keying's state untouched.
     */
    private fun refusedWithoutEffect(format: CompleteTrackFormat, keying: UnitAudioKeying, expected: String) {
        val rig = Rig()
        val refused = assertFailsWith<IllegalArgumentException>("refused at assembly") { compose(rig, keying = keying, audio = format) }
        assertEquals(expected, refused.message)
        assertEquals(0, rig.acquirer.begins(), "no acquisition begun")
        assertEquals(0, rig.acquirer.acquisitions(), "no resource opened")
        assertEquals(emptyList(), rig.cleanupNow(), "nothing closed or disposed")
        assertEquals(0, rig.deliveries.attempts, "no callback delivery attempted")
        assertTrue(rig.dispatcher.isAlive, "no dispatcher retired")
        assertEquals(emptyList(), rig.calls.all(), "no write, end, completion or drain")
        assertEquals(300, keying.retainedFrames, "the supplied keying's partial unit is untouched")
        assertEquals(listOf(1L), keying.onRead(keying.framesPerUnit - 300), "and its next unit is still unit 1")
    }

    /** The validator's facts for the two literal ASCs: 11 90 is 48 kHz, 12 10 is 44.1 kHz, both 1024-sample AAC LC units. */
    @Test
    fun k0_theAuthorityVectorsCarryTheirDeclaredFacts() {
        assertEquals(48_000, facts(format48k).sampleRate)
        assertEquals(1_024, facts(format48k).framesPerAccessUnit)
        assertEquals(44_100, facts(format44k).sampleRate)
        assertEquals(1_024, facts(format44k).framesPerAccessUnit)
    }

    @Test
    fun k1_aKeyingAt44kHzIsRefusedAgainstA48kHzFormat() {
        refusedWithoutEffect(
            format48k,
            keyingWithState(44_100, FRAMES_PER_UNIT, ORIGIN),
            "the audio keying's sample rate 44100 is not the validated AAC format's 48000",
        )
    }

    @Test
    fun k1b_aKeyingAt48kHzIsRefusedAgainstA44kHzFormat() {
        refusedWithoutEffect(
            format44k,
            keyingWithState(48_000, FRAMES_PER_UNIT, ORIGIN),
            "the audio keying's sample rate 48000 is not the validated AAC format's 44100",
        )
    }

    @Test
    fun k2_aKeyingWhoseUnitIsNot1024FramesIsRefusedAgainstAacLc() {
        refusedWithoutEffect(
            format48k,
            keyingWithState(48_000, 960, ORIGIN),
            "the audio keying's unit of 960 frames is not the validated AAC format's 1024",
        )
    }

    /**
     * A keying that agrees -- rate, unit and origin -- is retained as the
     * EXACT supplied object, its state untouched, and keys on its own
     * timeline: [ORIGIN] is nonzero, and at 44.1 kHz a unit is a fractional
     * 23 219.95 us, floored once over the whole offset.
     */
    private fun agreeingKeyingIsRetained(format: CompleteTrackFormat, unitOne: Long, unitThree: Long) {
        val rig = Rig()
        val keying = keyingWithState(facts(format).sampleRate, facts(format).framesPerAccessUnit, ORIGIN)
        val live = compose(rig, keying = keying, audio = format)
        assertSame(keying, live.audio.keying, "the supplied object, not a replacement")
        assertEquals(300, keying.retainedFrames, "assembly consumed no PCM")
        assertEquals(listOf(1L), keying.onRead(keying.framesPerUnit - 300), "the declared next unit completes")
        assertEquals(AudioKeyOutcome.Key(ORIGIN + unitOne), keying.keyFor(1L))
        assertEquals(AudioKeyOutcome.Key(ORIGIN + unitThree), keying.keyFor(3L))
        assertEquals(0, rig.acquirer.begins(), "and assembly began nothing")
    }

    @Test
    fun k3_aKeyingThatAgreesAt48kHzIsTheSuppliedObjectWithItsState() {
        // 1024 / 48 000 s = 21 333.33 us; three units 64 000 us.
        agreeingKeyingIsRetained(format48k, unitOne = 21_333L, unitThree = 64_000L)
    }

    @Test
    fun k3b_aKeyingThatAgreesAt44kHzIsTheSuppliedObjectWithItsState() {
        // 1024 / 44 100 s = 23 219.95 us, floored; three units 69 659.86 us, floored ONCE.
        agreeingKeyingIsRetained(format44k, unitOne = 23_219L, unitThree = 69_659L)
    }

    /**
     * Assembly acquires NOTHING. A composition that is built and never run
     * leaves only objects the caller drops: no driver entered, no resource
     * opened, no dispatcher touched, and the supplied keying not advanced.
     */
    @Test
    fun c5_aCompositionThatIsNeverRunAcquiresNothing() = runTest {
        val rig = Rig()
        val live = compose(rig)
        assertEquals(0, rig.acquirer.begins(), "the driver was never entered")
        assertEquals(0, rig.acquirer.acquisitions(), "and nothing was opened")
        assertEquals(emptyList(), rig.cleanupNow(), "so there was nothing to close")
        assertTrue(rig.dispatcher.isAlive, "and no dispatcher was retired")
        assertEquals(emptyList(), rig.calls.all(), "the sender was never asked for anything")
        assertEquals(emptyMap(), live.record.snapshot(), "and the record is the one that was supplied, untouched")
        assertSame(rig.record, live.record)
        assertEquals(0, rig.keying.retainedFrames, "assembly read no PCM")
        assertEquals(listOf(0L), assertNotNull(live.audio.keying).onRead(FRAMES_PER_UNIT), "so the driver's first read is unit 0")
        assertEquals(MediaKind.Video, live.video.kind)
        assertEquals(MediaKind.Audio, live.audio.kind)
        assertNull(live.video.keying, "video has no frame-count keying step")
        assertEquals(GENERATION, live.video.generation)
        assertEquals(GENERATION, live.audio.generation)
    }

    /**
     * Generations must agree, and the disagreement is refused BEFORE anything
     * is acquired -- not left for a record that would silently refuse every
     * fact it is later given.
     */
    @Test
    fun c6_aMismatchedGenerationIsRefusedBeforeAnythingIsAcquired() = runTest {
        val wrongPermissions = Rig()
        val byPermission =
            assertFailsWith<IllegalArgumentException> { compose(wrongPermissions, Granted(GENERATION + 1L)) }
        assertEquals(
            "the permission flow is for generation ${GENERATION + 1L}, not $GENERATION",
            byPermission.message,
        )
        assertEquals(0, wrongPermissions.acquirer.begins())

        val wrongRecord = Rig(record = CaptureDiscardRecord(GENERATION + 2L))
        val byRecord = assertFailsWith<IllegalArgumentException> { compose(wrongRecord) }
        assertEquals(
            "the discard record is for generation ${GENERATION + 2L}, not $GENERATION",
            byRecord.message,
        )
        assertEquals(0, wrongRecord.acquirer.begins(), "and nothing was begun on the way to refusing")
    }

    /**
     * A second run while the first is still live is the accepted session's
     * refusal, by name. It is not a second broadcast, it acquires nothing more,
     * and it does not stop or disturb the run that is already going.
     */
    @Test
    fun c7_aSecondRunIsRefusedAndLeavesTheFirstAlone() = runTest {
        val rig = Rig()
        val live = compose(rig)
        withRun(live.run) { owner ->
            runCurrent()
            assertEquals(1, rig.acquirer.begins(), "the first run entered the driver")
            assertFalse(owner.job.isCompleted, "and is still live")
            val second = live.run()
            val unfinished = assertIs<BroadcastOutcome.Unfinished>(second, "the second run is a named refusal")
            assertEquals(
                "the capture session refused to start: this session has already been started",
                unfinished.reason,
            )
            assertNull(unfinished.video, "it drained nothing")
            assertNull(unfinished.audio)
            runCurrent()
            assertEquals(1, rig.acquirer.begins(), "the driver was not entered again")
            assertEquals(FULL_ORDER.size, rig.acquirer.acquisitions(), "and nothing more was acquired")
            assertFalse(owner.job.isCompleted, "the first run is untouched by the refusal")
            assertEquals(emptyList(), rig.cleanupNow(), "nothing was closed behind it")
            assertTrue(rig.dispatcher.isAlive)
        }
    }

    /**
     * The fault relay is bound BEFORE acquisition can deliver a callback: a
     * codec error raised inside `begin`, before the first resource is opened,
     * reaches the session and ends the startup.
     *
     * An unbound relay would raise out of `begin` instead, so the run
     * RETURNING an outcome is the discriminator. The refusal names the
     * startup's own disposition rather than the fault: the accepted session
     * carries the fault's identity on its TERMINATION, and reports a startup
     * that a terminal decision ended first as `ENDED_BEFORE_READY`
     * (CaptureTopology.kt:491,503). That the fault landed on its own track is
     * read from the track it ended.
     */
    @Test
    fun c8_aFaultDuringBeginReachesTheSessionThatIsStarting() = runTest {
        val rig = Rig()
        val live = compose(rig)
        rig.acquirer.duringBegin = { live.video.handoff.onCodecError(GENERATION, ENCODER_FAILED) }
        val outcome = live.run()
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome, "the start ended by name, not by an unbound relay")
        assertEquals(
            "the capture session refused to start: the session ended before the startup was ready",
            unfinished.reason,
        )
        assertTrue(live.video.handoff.ended, "the fault ended the track it was raised on")
        assertEquals(
            HandoffOutcome.Ignored("this track has ended"),
            live.video.handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, VIDEO_PTS, CaptureContract.FLAG_KEY_FRAME)),
            "so a later output on it is ignored, releasing nothing",
        )
        assertEquals(emptyList(), rig.videoIndices.indices(), "nothing was released for it")
        assertEquals(emptyList(), rig.calls.all(), "nothing was published")
        assertEquals(emptyMap(), rig.record.snapshot(), "and nothing was admitted to discard")
    }

    /**
     * The same held-transfer geometry as c3, on the AUDIO track: each track's
     * codec lifetime is its own, so it is the audio lease that defers the
     * audio disposal while the video codec -- which nothing holds -- is
     * disposed where the ordered close reaches it.
     */
    @Test
    fun c9_aHeldAudioTransferDefersItsOwnCodec() = runTest {
        val reached = HostGate("the audio transfer reached its codec buffer")
        val release = HostGate("the row released the held audio transfer")
        val rig = Rig(audioHold = Triple(0, reached, release))
        val live = compose(rig)
        val worker =
            HostWorker("audio-transfer") {
                deliver(live.audio, rig.audioBuffers, 0, bytes(2, 3), ORIGIN, FIRST_CAPTURE)
            }
        settling(listOf(reached, release), listOf(worker)) {
            withRun(live.run) { owner ->
                runCurrent()
                worker.start()
                reached.await(worker)
                assertEquals(1, rig.audioCodec.outstandingLeases, "the held audio transfer holds its OWN codec")
                assertEquals(0, rig.videoCodec.outstandingLeases, "and holds nothing of the video codec's")

                owner.cancel("the user left")
                runCurrent()
                assertEquals(1, rig.audioCodec.outstandingLeases, "the audio lease outlives the close that asked")
                assertFalse(rig.audioCodec.isDisposed)
                assertTrue(rig.videoCodec.isDisposed, "while the codec nothing holds is disposed where it stands")

                release.openFully()
                worker.join()
                runCurrent()
                assertEquals(0, rig.audioCodec.outstandingLeases)
                assertTrue(rig.audioCodec.isDisposed, "and only the returned lease destroys it")
                assertEquals(1, rig.audioCodec.disposalCount, "exactly once")
                assertEquals(1, rig.videoCodec.disposalCount)
                assertEquals(listOf(0), rig.audioIndices.indices(), "its index went back to its own codec")
                assertEquals(emptyList(), rig.videoIndices.indices())
                assertFalse(rig.dispatcher.isAlive, "and the worker retired after the codec that ran on it")
            }
        }
        assertEquals(mapOf(MediaKind.Audio to 1), rig.record.snapshot(), "the discarded audio output is kept as a partial fact")
    }

    // ---- fixture qualification ---------------------------------------------------
    //
    // These do not measure the composition root. They measure the two ways
    // this file's own settlement could lie: a failure raised while a worker is
    // admitted, and a run that dies of something other than this row's
    // cancellation.

    /** A failure with a worker held at a gate settles that worker and leaves as itself. */
    @Test
    fun m1_aFailureWithAWorkerAdmittedStillSettlesIt() = runTest {
        val reached = HostGate("the worker was admitted")
        val release = HostGate("the row released the worker")
        val worker = HostWorker("admitted") { reached.open(); release.await() }
        val mine = AssertionError("the row failed with a worker admitted")
        val raised =
            assertFailsWith<AssertionError> {
                settling(listOf(reached, release), listOf(worker)) {
                    withRun({ CompletableDeferred<BroadcastOutcome>().await() }) {
                        runCurrent()
                        worker.start()
                        reached.await(worker)
                        throw mine
                    }
                }
            }
        assertTrue(recovers(raised, mine), "the row's own failure is what leaves: $raised")
        assertEquals(mine.message, raised.message)
        assertTrue(worker.isSettled, "and the worker it owned is finished, not stranded")
        assertNull(worker.failureNow(), "having been released rather than interrupted")
    }

    /**
     * A run that dies of its own INDEPENDENT failure is reported by the
     * settlement. This is not the carrier case -- a cleanup failure attached
     * to the row's own cancellation is [m5] and [m6].
     *
     * `isCancelled` cannot say this: kotlinx reports it for every exceptional
     * completion, so a run that failed on its own reads exactly like a run
     * this row cancelled. The settlement is what tells them apart, and the row
     * pins both halves of that.
     */
    @Test
    fun m2_aRunThatFailsOnItsOwnIsNotHiddenByCancellation() = runTest {
        val failure = IllegalStateException("the run failed on its own")
        val started = CompletableDeferred<Unit>()
        val raised =
            assertFailsWith<IllegalStateException> {
                withRun({ started.complete(Unit); throw failure }) { owner ->
                    runCurrent()
                    withTimeoutOrNull(BOUNDED) { started.await() } ?: throw AssertionError("the run never began")
                    withTimeoutOrNull(BOUNDED) { owner.job.join() } ?: throw AssertionError("the run never settled")
                    assertTrue(owner.job.isCancelled, "a job that FAILED reads as cancelled: the flag cannot tell them apart")
                    assertTrue(recovers(assertNotNull(owner.observed().exceptionOrNull()), failure))
                }
            }
        assertTrue(recovers(raised, failure), "and the settlement hands back what it died of: $raised")
        assertEquals(failure.message, raised.message)
    }

    /**
     * The carrier case, with a body that SUCCEEDED: accepted cleanup keeps the
     * cancellation and attaches what it found to it, so a settlement that
     * treats "this is my own cancellation" as "nothing went wrong" throws the
     * cleanup failure away. This row fails if it does.
     */
    @Test
    fun m5_aCleanupFailureCarriedByTheRowsOwnCancellationIsReported() = runTest {
        val cleanup = IllegalStateException("the run's own cleanup failed")
        val parked = CompletableDeferred<Unit>()
        val raised =
            assertFailsWith<IllegalStateException> {
                withRun(carrying(parked, cleanup)) {
                    runCurrent()
                    withTimeoutOrNull(BOUNDED) { parked.await() } ?: throw AssertionError("the run never parked")
                }
            }
        assertTrue(recovers(raised, cleanup), "the cleanup fact leaves by itself when the body succeeded: $raised")
        assertEquals(cleanup.message, raised.message)
    }

    /** The same carrier, with a body that FAILED first: the primary stands and the fact is attached. */
    @Test
    fun m6_aCarriedCleanupFailureIsAttachedToTheBodysOwnFailure() = runTest {
        val cleanup = IllegalStateException("the run's own cleanup failed")
        val parked = CompletableDeferred<Unit>()
        val mine = AssertionError("the row failed before the settlement")
        val raised =
            assertFailsWith<AssertionError> {
                withRun(carrying(parked, cleanup)) {
                    runCurrent()
                    withTimeoutOrNull(BOUNDED) { parked.await() } ?: throw AssertionError("the run never parked")
                    throw mine
                }
            }
        assertTrue(recovers(raised, mine), "the body's own failure stays primary: $raised")
        assertTrue(mine.suppressed.any { it === cleanup }, "with the cleanup fact attached, not discarded")
        assertTrue(mine.suppressed.none { it === mine }, "and nothing suppressed onto itself")
    }

    /** The clean control: a cancellation that carries nothing settles quietly. */
    @Test
    fun m7_aCleanCancellationSettlesQuietly() = runTest {
        var body = false
        withRun({ CompletableDeferred<BroadcastOutcome>().await() }) { owner ->
            runCurrent()
            assertFalse(owner.job.isCompleted, "the run is still live when the row leaves")
            body = true
        }
        assertTrue(body, "and the row's body ran to its end")
    }

    /** A run parked until cancelled, whose cleanup attaches [cleanup] to that cancellation. */
    private fun carrying(parked: CompletableDeferred<Unit>, cleanup: Throwable): suspend () -> BroadcastOutcome = {
        try {
            parked.complete(Unit)
            CompletableDeferred<BroadcastOutcome>().await()
        } catch (cancelled: CancellationException) {
            cancelled.addSuppressed(cleanup)
            throw cancelled
        }
    }

    /**
     * The held close boundary, through the REAL OrderedCaptureOwnership and
     * the accepted handoff: one leased codec defers its own disposal and the
     * worker it runs on, and neither defers the codec nothing is holding.
     *
     * This is the mechanism behind c3's inventory. It is not a composition and
     * proves nothing about the unimplemented root.
     */
    @Test
    fun m8_onlyTheLeasedCodecAndItsWorkerAreDeferredByAHeldTransfer() {
        val reached = HostGate("the transfer reached the codec buffer")
        val release = HostGate("the control released the transfer")
        val rig = Rig(videoHold = Triple(0, reached, release))
        val worker = closing(rig, reached, release) {
            assertEquals(
                listOf("close:audio-codec", "dispose:audio-codec", "close:video-codec"),
                rig.cleanupNow(),
                "the unleased codec is disposed where it stands; the leased one only asked",
            )
            assertEquals(1, rig.videoCodec.outstandingLeases)
            assertFalse(rig.videoCodec.isDisposed)
            assertEquals(1, rig.audioCodec.disposalCount)
            assertTrue(rig.dispatcher.isAlive, "and the worker the held callback runs on is retained")
        }
        assertEquals(
            listOf(
                "close:audio-codec",
                "dispose:audio-codec",
                "close:video-codec",
                "dispose:video-codec",
                "close:worker",
                "quit:dispatcher",
            ),
            rig.cleanupNow(),
            "the returned lease disposes the codec, and only then is the worker retired",
        )
        assertFalse(rig.dispatcher.isAlive)
        assertEquals(1, rig.videoCodec.disposalCount)
        assertEquals(listOf(0), rig.videoIndices.indices())
        assertTrue(worker.isSettled)
    }

    /**
     * The same close with the worker retention OMITTED -- the wiring c3's
     * dispatcher assertion forbids. The worker is retired, and the dispatcher
     * the held callback is delivering on is quit, while that callback is still
     * inside the codec.
     */
    @Test
    fun m9_omittingTheWorkerRetentionRetiresTheDispatcherUnderTheCallback() {
        val reached = HostGate("the transfer reached the codec buffer")
        val release = HostGate("the control released the transfer")
        val rig = Rig(videoHold = Triple(0, reached, release), retainWorker = false)
        closing(rig, reached, release) {
            assertEquals(
                listOf("close:audio-codec", "dispose:audio-codec", "close:video-codec", "close:worker", "quit:dispatcher"),
                rig.cleanupNow(),
                "nothing retained the worker, so it went with the rest",
            )
            assertFalse(rig.dispatcher.isAlive, "the callback is delivering onto a quit dispatcher")
            assertEquals(1, rig.videoCodec.outstandingLeases, "even though its codec is still leased")
        }
    }

    /** The delegate refuses a retention it has no ownership for, and a second ownership. */
    @Test
    fun m10_theWorkerDelegateRefusesUseBeforeBindingAndAConflictingRebinding() {
        val late = LateWorker()
        assertFailsWith<IllegalStateException> { late.retainWorker() }
        val first = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        val second = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        late.bind(first)
        late.bind(first)
        assertFailsWith<IllegalArgumentException> { late.bind(second) }
        assertTrue(late.retainWorker(), "and once bound it really delegates")
        late.releaseWorker()
    }

    /**
     * A failure raised AFTER the transfer is held and BEFORE the measured
     * close still closes every handle that was acquired. The old helper closed
     * the ownership only on its success path, so this left five installed
     * handles open with the dispatcher still alive.
     */
    @Test
    fun m11_aFailureBeforeTheMeasuredCloseStillClosesWhatWasAcquired() {
        val reached = HostGate("the transfer reached the codec buffer")
        val release = HostGate("the control released the transfer")
        val rig = Rig(videoHold = Triple(0, reached, release))
        val mine = AssertionError("the control failed before the close")
        val raised =
            assertFailsWith<AssertionError> {
                closing(rig, reached, release, beforeClose = { throw mine }) {
                    throw AssertionError("the held boundary must not be reached")
                }
            }
        assertSame(mine, raised, "the control's own failure is what leaves")
        assertEquals(
            listOf(
                "close:audio-codec",
                "dispose:audio-codec",
                "close:video-codec",
                "dispose:video-codec",
                "close:worker",
                "quit:dispatcher",
            ),
            rig.cleanupNow(),
            "every acquired handle closed, once, in the accepted order",
        )
        assertEquals(listOf("camera", "microphone"), rig.others.map { it.toString() }.sorted())
        assertFalse(rig.dispatcher.isAlive, "and no dispatcher is left alive")
        assertEquals(1, rig.videoCodec.disposalCount)
        assertEquals(1, rig.audioCodec.disposalCount)
        assertEquals(0, rig.videoCodec.outstandingLeases, "the held callback settled before the cleanup close")
    }

    /**
     * A setup failure BEFORE any callback thread exists -- one resource in the
     * order cannot be opened -- still closes the handles installed before it.
     */
    @Test
    fun m12_aPartialAcquisitionClosesWhatWasInstalledBeforeIt() {
        val reached = HostGate("unreached")
        val release = HostGate("unreleased")
        val rig = Rig(videoHold = Triple(0, reached, release), failAt = CaptureResource.VideoEncoder)
        val raised =
            assertFailsWith<AssertionError> {
                closing(rig, reached, release) { throw AssertionError("the held boundary must not be reached") }
            }
        assertTrue(
            raised.message.orEmpty().contains("every resource in the order was acquired"),
            "the setup failure is what leaves: ${raised.message}",
        )
        assertEquals(listOf("close:worker", "quit:dispatcher"), rig.cleanupNow(), "the worker installed before it closed")
        assertEquals(listOf("camera"), rig.others.map { it.toString() }, "and so did the camera")
        assertFalse(rig.dispatcher.isAlive, "no dispatcher is left alive")
        assertEquals(0, rig.videoCodec.disposalCount, "the codec that never opened is not disposed")
        assertFalse(rig.videoCodec.everLeased)
    }

    /**
     * Acquires [rig]'s resources through a real ownership, parks one video
     * transfer inside the codec, closes the ownership, runs [held] at that
     * boundary and then releases.
     *
     * The ownership is owned from the moment it EXISTS, not from the moment
     * the first worker starts: a setup failure before any callback thread
     * exists, or a failure raised before the measured close, still closes
     * every handle that was installed. The close at the measured boundary is
     * the one the rows read; the one on the way out is idempotent and exists
     * so nothing is left open. It never replaces the failure that is leaving.
     */
    private fun closing(
        rig: Rig,
        reached: HostGate,
        release: HostGate,
        beforeClose: () -> Unit = {},
        held: () -> Unit,
    ): HostWorker {
        val ownership = OrderedCaptureOwnership(GENERATION, rig.dispatcher, LateLog())
        val workers = mutableListOf<HostWorker>()
        var primary: Throwable? = null
        try {
            val steps =
                object : AcquisitionSteps {
                    override fun submitted(resource: CaptureResource) = Unit

                    override fun settled(resource: CaptureResource, outcome: AdoptOutcome) = Unit
                }
            assertNull(rig.acquirer.begin(ownership, steps), "every resource in the order was acquired")
            val ledger = HostLedger(MediaKind.Video, GENERATION)
            val handoff =
                BoundedCaptureHandoff(
                    kind = MediaKind.Video,
                    generation = GENERATION,
                    buffers = rig.videoAccess,
                    owner = rig.videoIndices,
                    ledger = ledger,
                    faults = HostFaults(),
                    capacity = CAPACITY,
                    maxOutputBytes = HOST_MAX_OUTPUT_BYTES,
                    lifetime = rig.videoCodec,
                )
            val payload = bytes(1, 6)
            rig.videoBuffers.put(0, ByteBuffer.wrap(payload))
            assertEquals(
                ObserveOutcome.Recorded,
                ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(VIDEO_PTS, VIDEO_CAPTURE)),
            )
            val worker =
                HostWorker("transfer") {
                    handoff.onOutput(GENERATION, CodecOutput(0, 0, payload.size, VIDEO_PTS, CaptureContract.FLAG_KEY_FRAME))
                }
            workers.add(worker)
            worker.start()
            reached.await(worker)
            beforeClose()
            ownership.close(null)
            held()
            release.openFully()
            worker.join()
            return worker
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
            val found = mutableListOf<Throwable>()
            reached.openFully()
            release.openFully()
            // The callbacks first: a lease still held would defer the very
            // disposal this close is here to make happen.
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
            try {
                ownership.close(null)?.let { found.add(it) }
            } catch (failure: RuntimeException) {
                found.add(failure)
            } catch (error: Error) {
                found.add(error)
            }
            val leaving = primary
            if (leaving != null) {
                found.forEach { fact ->
                    if (fact !== leaving && leaving.suppressed.none { it === fact }) leaving.addSuppressed(fact)
                }
            } else {
                found.firstOrNull()?.let { first ->
                    found.drop(1).forEach { if (it !== first) first.addSuppressed(it) }
                    throw first
                }
            }
        }
    }

    /**
     * The scripted buffer gate, qualified against the ACCEPTED handoff: a
     * callback parked in the fetch really is inside the codec under a granted
     * lease, a disposal its owner asks for while it is held is deferred, and
     * the last lease returned is what destroys the codec -- once.
     *
     * This proves the c3 mechanism. It is not a composition and proves nothing
     * about the unimplemented root.
     */
    @Test
    fun m3_theHeldBufferFetchReallyHoldsACodecLease() {
        val trace = mutableListOf<String>()
        val codec = HostCodec(trace, name = "video-codec")
        val released = held(codec) { reached, release, worker, releases ->
            worker.start()
            reached.await(worker)
            assertEquals(1, codec.outstandingLeases, "the fetch runs under a granted lease")
            codec.requestDisposal()
            assertFalse(codec.isDisposed, "which defers the disposal its owner asked for")
            assertEquals(emptyList(), trace.filter { it.startsWith("dispose:") })
            release.openFully()
            worker.join()
            assertEquals(0, codec.outstandingLeases, "the last lease came back")
            assertTrue(codec.isDisposed, "and that is what destroyed the codec")
            assertEquals(1, codec.disposalCount, "exactly once")
            releases
        }
        assertEquals(listOf(0), released.indices(), "the held index went back")
    }

    /**
     * The same wiring on `CodecLifetime.RETAINED` -- what a composition that
     * forgot to pass the codec's own lifetime would build. Nothing leases the
     * codec, so nothing defers its disposal and a callback keeps reading a
     * codec its owner has already destroyed. This is what gives the lease and
     * disposal oracles in c1/c3 their teeth.
     */
    @Test
    fun m4_aHandoffLeftOnTheRetainedLifetimeLeasesNothing() {
        val trace = mutableListOf<String>()
        val codec = HostCodec(trace, name = "video-codec")
        held(codec, lifetime = CodecLifetime.RETAINED) { reached, release, worker, _ ->
            worker.start()
            reached.await(worker)
            assertEquals(0, codec.outstandingLeases, "the codec the owner closes is not the one the handoff leases")
            codec.requestDisposal()
            assertTrue(codec.isDisposed, "so it is destroyed with a callback still inside it")
            release.openFully()
            worker.join()
            assertFalse(codec.everLeased, "and nothing ever leased it at all")
        }
    }

    /**
     * One accepted handoff over one held buffer fetch, settled on every path.
     * [body] runs with the transfer worker unstarted.
     */
    private fun <T> held(
        codec: HostCodecOwner,
        lifetime: CodecLifetime = codec,
        body: (HostGate, HostGate, HostWorker, HostReleases) -> T,
    ): T {
        val reached = HostGate("the transfer reached the codec buffer")
        val release = HostGate("the control released the transfer")
        val buffers = HostBuffers()
        val releases = HostReleases()
        val ledger = HostLedger(MediaKind.Video, GENERATION)
        val handoff =
            BoundedCaptureHandoff(
                kind = MediaKind.Video,
                generation = GENERATION,
                buffers = GatedBuffers(buffers, 0, reached, release),
                owner = releases,
                ledger = ledger,
                faults = HostFaults(),
                capacity = CAPACITY,
                maxOutputBytes = HOST_MAX_OUTPUT_BYTES,
                lifetime = lifetime,
            )
        val payload = bytes(1, 6)
        buffers.put(0, ByteBuffer.wrap(payload))
        assertEquals(
            ObserveOutcome.Recorded,
            ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(VIDEO_PTS, VIDEO_CAPTURE)),
        )
        val worker =
            HostWorker("transfer") {
                handoff.onOutput(GENERATION, CodecOutput(0, 0, payload.size, VIDEO_PTS, CaptureContract.FLAG_KEY_FRAME))
            }
        var outcome: T? = null
        withSettlement(listOf(reached, release), listOf(worker)) { outcome = body(reached, release, worker, releases) }
        @Suppress("UNCHECKED_CAST")
        return outcome as T
    }

    // ---- d: the deferred composition -- formats observed after assembly -------------

    /**
     * The deferred assembly over the row's scripted sender tracks: the SAME
     * environment, keying and record as [compose], with both format holders
     * validating through the fixture's scripted utilities.
     */
    private fun composeDeferred(
        rig: Rig,
        tracks: ScriptedTracks,
        keying: AudioKeying = rig.keying,
        videoUtility: AvcRecordUtility = ScriptedVideoUtility,
    ): DeferredLiveCapture =
        composeDeferredBroadcast(
            generation = GENERATION,
            permissions = Granted(GENERATION),
            cameraFacts = CaptureCapability.Available,
            audioFacts = CaptureCapability.Available,
            calibration = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L),
            origin = ORIGIN,
            audioKeying = keying,
            capacity = CAPACITY,
            maxOutputBytes = HOST_MAX_OUTPUT_BYTES,
            maxNalUnits = HOST_MAX_NAL_UNITS,
            videoName = VIDEO_CONFIG.name,
            audioName = AUDIO_CONFIG.name,
            formatBounds = FIXTURE_BOUNDS,
            formatBudget = FORMAT_BUDGET,
            environment = rig.environment,
            tracks = tracks,
            completion = rig.sinks.completion,
            record = rig.record,
            writeTimeout = WRITE_TIMEOUT,
            drainTimeout = DRAIN_TIMEOUT,
            videoUtility = videoUtility,
            audioUtility = ScriptedAudioUtility,
        )

    private fun addedConfigs(tracks: ScriptedTracks) = tracks.all().filterIsInstance<Effect.Add>().map { it.config }

    /**
     * The deferred run, settled; [body] drives the platform inside it. The
     * owner's own result is returned; a failure of the row itself -- an
     * assertion in [body], or an owner that did not settle -- leaves as
     * itself rather than as a missing result.
     */
    private suspend fun kotlinx.coroutines.test.TestScope.runDeferred(live: DeferredLiveCapture, body: suspend (RunOwner) -> Unit): Result<BroadcastOutcome> {
        var outcome: Result<BroadcastOutcome>? = null
        val wrapper =
            runCatching {
                withRun(live.run) { owner ->
                    runCurrent()
                    body(owner)
                    runCurrent()
                    outcome = settled(owner)
                }
            }
        return outcome ?: throw (wrapper.exceptionOrNull() ?: AssertionError("the run produced no result"))
    }

    @Test
    fun d1_eitherArrivalOrderAddsBothExactConfigurationsOnlyOnceBothArriveAndBindsEachHandleToItsLane() = runTest {
        for (audioFirst in listOf(true, false)) {
            val rig = Rig()
            val v = HostTrack("v")
            val a = HostTrack("a")
            val tracks = ScriptedTracks().answers(v).answers(a)
            val live = composeDeferred(rig, tracks)
            assertSame(rig.keying, live.audio.keying, "the supplied keying, not a replacement")
            assertEquals(0, rig.keying.retainedFrames, "assembly read nothing")
            var addsAfterFirst: List<Any>? = null
            val payload = bytes(2, 3)
            val outcome =
                runDeferred(live) {
                    val first = if (audioFirst) live.audioFormat.onOutputFormat(audioObservation()) else live.videoFormat.onOutputFormat(videoObservation())
                    assertIs<FormatOutcome.Resolved>(first)
                    runCurrent()
                    addsAfterFirst = addedConfigs(tracks)
                    val second = if (audioFirst) live.videoFormat.onOutputFormat(videoObservation()) else live.audioFormat.onOutputFormat(audioObservation())
                    assertIs<FormatOutcome.Resolved>(second)
                    runCurrent()
                    assertEquals(HandoffOutcome.Queued, deliver(live.video, rig.videoBuffers, 0, VIDEO_ANNEXB.copyOf(), VIDEO_PTS, VIDEO_CAPTURE))
                    runCurrent()
                    assertEquals(listOf(0L), rig.keying.onRead(FRAMES_PER_UNIT), "the driver's first read completes unit 0")
                    val unitZero = assertIs<AudioKeyOutcome.Key>(rig.keying.keyFor(0L)).presentationTimeUs
                    assertEquals(HandoffOutcome.Queued, deliver(live.audio, rig.audioBuffers, 0, payload, unitZero, FIRST_CAPTURE))
                    runCurrent()
                    assertEquals(HandoffOutcome.Queued, deliverEnd(live.video, rig.videoBuffers, 1))
                    assertEquals(HandoffOutcome.Queued, deliverEnd(live.audio, rig.audioBuffers, 1))
                }
            val label = if (audioFirst) "audio first" else "video first"
            assertEquals(emptyList(), addsAfterFirst, "$label: nothing added before both formats arrived")
            assertIs<BroadcastOutcome.Published>(outcome.getOrThrow(), label)
            assertEquals(listOf(VIDEO_FORMAT_CONFIG, AUDIO_FORMAT_CONFIG), addedConfigs(tracks), "$label: the exact validated configurations, video then audio")
            val writes = tracks.all().filterIsInstance<Effect.Write>()
            assertEquals(2, writes.size)
            assertSame(v, writes[0].handle, "$label: the video lane writes through the video handle")
            assertContentEquals(VIDEO_AVC1, writes[0].obj.payload.bytes())
            assertSame(a, writes[1].handle, "$label: and the audio lane through the audio handle")
            assertContentEquals(payload, writes[1].obj.payload.bytes())
            assertEquals(MediaMicros(0uL), writes[1].obj.presentationTime, "audio keyed by the supplied keying on the shared origin")
            assertEquals(listOf<Call>(Call.Drained(DRAIN_TIMEOUT), Call.Completed, Call.Drained(DRAIN_TIMEOUT)), rig.calls.all())
            assertEquals(listOf("dispose:audio-codec", "dispose:video-codec"), rig.disposals())
        }
    }

    /**
     * The supplied keying disagrees with the AAC facts that arrive: named, and
     * refused BEFORE either add; the capture that was started is stopped and
     * cleaned up; nothing is completed; the keying's reads are untouched.
     */
    private suspend fun kotlinx.coroutines.test.TestScope.deferredKeyingRefused(keying: UnitAudioKeying, expected: String) {
        val rig = Rig()
        val tracks = ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))
        val live = composeDeferred(rig, tracks, keying = keying)
        assertSame(keying, live.audio.keying)
        val outcome =
            runDeferred(live) {
                live.videoFormat.onOutputFormat(videoObservation())
                live.audioFormat.onOutputFormat(audioObservation())
                runCurrent()
                assertEquals(emptyList(), tracks.all(), "neither track was added once both formats had arrived")
            }
        assertEquals(emptyList(), tracks.all(), "neither track was added")
        assertEquals(BroadcastOutcome.Unfinished(expected, null, null), outcome.getOrThrow(), "refused by name")
        assertEquals(emptyList(), rig.calls.all(), "no completion or drain")
        assertEquals(listOf("dispose:audio-codec", "dispose:video-codec"), rig.disposals(), "the started capture was cleaned up, once")
        assertEquals(300, keying.retainedFrames, "the keying's partial unit is untouched")
        assertEquals(listOf(1L), keying.onRead(keying.framesPerUnit - 300), "and its next unit is still unit 1")
    }

    @Test
    fun d2_aKeyingWhoseRateDisagreesIsRefusedBeforeEitherAdd() = runTest {
        deferredKeyingRefused(keyingWithState(44_100, FRAMES_PER_UNIT, ORIGIN), "the audio keying's sample rate 44100 is not the validated AAC format's 48000")
    }

    @Test
    fun d2b_aKeyingWhoseUnitGeometryDisagreesIsRefusedBeforeEitherAdd() = runTest {
        deferredKeyingRefused(keyingWithState(48_000, 960, ORIGIN), "the audio keying's unit of 960 frames is not the validated AAC format's 1024")
    }

    @Test
    fun d3_aKeyingForAnotherOriginIsRefusedAtAssemblyWithoutEffect() = runTest {
        val rig = Rig()
        val tracks = ScriptedTracks()
        val refused = assertFailsWith<IllegalArgumentException> { composeDeferred(rig, tracks, keying = HostAudioKeying(SAMPLE_RATE, FRAMES_PER_UNIT, ORIGIN + 1L)) }
        assertEquals("the audio keying is keyed to origin ${ORIGIN + 1L}, not the broadcast origin $ORIGIN", refused.message)
        assertEquals(0, rig.acquirer.begins(), "nothing begun")
        assertEquals(emptyList(), rig.cleanupNow())
        assertEquals(emptyList(), tracks.all())
    }

    /** Formats observed before the run starts are admitted by the same session and await the run's own adds. */
    @Test
    fun d4_formatsObservedBeforeTheRunAreHeldUntilItsAdds() = runTest {
        val rig = Rig()
        val tracks = ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))
        val live = composeDeferred(rig, tracks)
        assertIs<FormatOutcome.Resolved>(live.videoFormat.onOutputFormat(videoObservation()))
        assertIs<FormatOutcome.Resolved>(live.audioFormat.onOutputFormat(audioObservation()))
        assertEquals(emptyList(), tracks.all(), "nothing added before the run")
        assertEquals(0, rig.acquirer.begins(), "and nothing begun")
        val outcome =
            runDeferred(live) {
                assertEquals(HandoffOutcome.Queued, deliverEnd(live.video, rig.videoBuffers, 0))
                assertEquals(HandoffOutcome.Queued, deliverEnd(live.audio, rig.audioBuffers, 0))
            }
        assertIs<BroadcastOutcome.Published>(outcome.getOrThrow())
        assertEquals(listOf(VIDEO_FORMAT_CONFIG, AUDIO_FORMAT_CONFIG), addedConfigs(tracks))
    }

    /**
     * Formats delivered from INSIDE the driver's `begin`, before its first
     * acquisition, reach the holders already bound to this session; nothing
     * is added inside that callback, and the run then adds and publishes.
     */
    @Test
    fun d8_formatsDeliveredDuringBeginUseTheBoundHoldersBeforeAnyAcquisition() = runTest {
        val rig = Rig()
        val tracks = ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))
        val live = composeDeferred(rig, tracks)
        var entered = false
        rig.acquirer.duringBegin = {
            assertEquals(0, rig.acquirer.acquisitions(), "before the first acquisition")
            assertEquals(emptyList(), addedConfigs(tracks))
            assertIs<FormatOutcome.Resolved>(live.audioFormat.onOutputFormat(audioObservation()))
            assertIs<FormatOutcome.Resolved>(live.videoFormat.onOutputFormat(videoObservation()))
            assertEquals(emptyList(), addedConfigs(tracks), "nothing is added inside the driver's callback")
            entered = true
        }
        val outcome =
            runDeferred(live) {
                assertTrue(entered, "the formats were delivered during begin")
                assertEquals(HandoffOutcome.Queued, deliverEnd(live.video, rig.videoBuffers, 0))
                assertEquals(HandoffOutcome.Queued, deliverEnd(live.audio, rig.audioBuffers, 0))
            }
        assertIs<BroadcastOutcome.Published>(outcome.getOrThrow())
        assertEquals(listOf(VIDEO_FORMAT_CONFIG, AUDIO_FORMAT_CONFIG), addedConfigs(tracks))
        assertEquals(listOf("dispose:audio-codec", "dispose:video-codec"), rig.disposals())
    }

    /** An unexpected validation failure keeps the accepted contract: the original to the caller and to the owner, nothing added. */
    @Test
    fun d5_anUnexpectedFormatCallbackFailureIsTheOwnersFailureAsItself() = runTest {
        val rig = Rig()
        val tracks = ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))
        val original = IllegalStateException("the codec utility failed unexpectedly")
        val raising =
            object : AvcRecordUtility by ScriptedVideoUtility {
                override fun buildInitData(source: org.openmoq.moq5.CodecConfigSource, bytes: org.openmoq.moq5.MediaBytes, nalLengthSize: Int): UtilityOutcome<org.openmoq.moq5.MediaBytes> = throw original
            }
        val live = composeDeferred(rig, tracks, videoUtility = raising)
        var callerSaw: Throwable? = null
        val outcome = runDeferred(live) { callerSaw = runCatching { live.videoFormat.onOutputFormat(videoObservation()) }.exceptionOrNull() }
        assertSame(original, callerSaw, "the callback's caller gets the original")
        val failure = outcome.exceptionOrNull()
        assertTrue(generateSequence(failure) { it.cause }.any { it === original }, "and so does the owner: $outcome")
        assertEquals(emptyList(), tracks.all(), "nothing added")
        assertEquals(emptyList(), rig.calls.all(), "nothing completed")
        assertEquals(listOf("dispose:audio-codec", "dispose:video-codec"), rig.disposals())
    }

    @Test
    fun d6_cancellationWhileWaitingForFormatsStaysTheOwnersOwnAndAddsNothing() = runTest {
        val rig = Rig()
        val tracks = ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a"))
        val live = composeDeferred(rig, tracks)
        var why: Throwable? = null
        var run: RunOwner? = null
        val outcome =
            runDeferred(live) { owner ->
                live.videoFormat.onOutputFormat(videoObservation())
                run = owner
                why = owner.cancel("the user left while the audio format was pending")
            }
        val failure = outcome.exceptionOrNull()
        assertTrue(checkNotNull(run).issued(checkNotNull(failure)) && generateSequence(failure) { it.cause }.any { it === why }, "the owner's own cancellation: $failure")
        assertSame(FormatOutcome.Ended, live.audioFormat.onOutputFormat(audioObservation()), "a format arriving afterwards is not admitted")
        assertEquals(emptyList(), tracks.all())
        assertEquals(emptyList(), rig.calls.all())
        assertEquals(listOf("dispose:audio-codec", "dispose:video-codec"), rig.disposals())
    }

    /** Later formats go through the accepted holder authority: equal is inert, changed is the track's format fault. */
    @Test
    fun d7_laterFormatsUseTheHolderAuthorityEqualIsInertAndChangedEndsTheBroadcast() = runTest {
        val rig = Rig()
        val v = HostTrack("v")
        val a = HostTrack("a")
        val tracks = ScriptedTracks().answers(v).answers(a)
        val live = composeDeferred(rig, tracks)
        var equal: FormatOutcome? = null
        var changed: FormatOutcome? = null
        val outcome =
            runDeferred(live) {
                live.videoFormat.onOutputFormat(videoObservation())
                live.audioFormat.onOutputFormat(audioObservation())
                runCurrent()
                equal = live.audioFormat.onOutputFormat(audioObservation())
                changed = live.videoFormat.onOutputFormat(videoObservation(width = 1280))
            }
        assertSame(FormatOutcome.Equal, equal)
        val fault = assertIs<FormatOutcome.Changed>(changed).fault
        assertEquals(CaptureFault.Format(MediaKind.Video, "the video output format changed after it was validated"), fault)
        assertIs<BroadcastOutcome.Unfinished>(outcome.getOrThrow())
        assertEquals(listOf(VIDEO_FORMAT_CONFIG, AUDIO_FORMAT_CONFIG), addedConfigs(tracks), "never re-added with the new format")
        assertEquals(listOf("add:video", "add:audio", "remove:v", "remove:a"), tracks.labels(), "both handles retired")
        assertEquals(emptyList(), rig.calls.all(), "never completed")
    }

    private companion object {
        const val GENERATION = 1L
        const val CAPACITY = 4
        const val BASE_NANOS = 1_000_000_000L
        const val ORIGIN = 1_000_000L
        const val EPOCH = 1_700_000_000_000_000uL
        const val SAMPLE_RATE = 48_000
        const val FRAMES_PER_UNIT = 1_024
        const val VIDEO_PTS = ORIGIN + 33_333L
        const val VIDEO_CAPTURE = BASE_NANOS + 40_000_000L
        const val FIRST_CAPTURE = BASE_NANOS + 38_000_000L
        const val SECOND_CAPTURE = BASE_NANOS + 60_000_000L
        const val ENCODER_FAILED = "the encoder failed while the session was starting"
        val FULL_ORDER =
            listOf(
                CaptureResource.Worker,
                CaptureResource.Camera,
                CaptureResource.VideoEncoder,
                CaptureResource.Microphone,
                CaptureResource.AudioEncoder,
            )
        val WRITE_TIMEOUT = 1.seconds
        val DRAIN_TIMEOUT = 5.seconds
        val BOUNDED = 30.seconds
        val FORMAT_BUDGET = 5.seconds
    }
}
