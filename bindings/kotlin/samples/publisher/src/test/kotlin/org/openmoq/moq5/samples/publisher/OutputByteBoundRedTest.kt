package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.WriteOutcome

/**
 * The per-output byte bound in front of the copy: a live, otherwise
 * admissible callback whose `size` exceeds the declared `maxOutputBytes` is
 * refused by name at ADMISSION -- before the platform buffer is fetched,
 * before any byte is copied, before the ledger is claimed, before a slot is
 * reserved -- through the same refusal path every other admission refusal
 * takes. `g` rows measure this file's own oracles; `b` rows measure the real
 * [BoundedCaptureHandoff], the real session route and the real composition
 * against that contract.
 *
 * What this bounds is the payload population the handoff holds: queued
 * outputs plus reserved transfer slots, each at most `maxOutputBytes`.
 * Dequeued copies, conversion scratch, configuration snapshots, object
 * overhead and the codec's own buffers are not counted here.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class OutputByteBoundRedTest {

    private class HandoffRig(val kind: MediaKind, throwOnRelease: Set<Int> = emptySet(), lifetime: CodecLifetime = CodecLifetime.RETAINED, capacity: Int = CAPACITY) {
        val buffers = HostBuffers()
        val releases = HostReleases(throwOnRelease)
        val faults = HostFaults()
        val ledger = HostLedger(kind, GENERATION)
        val handoff = BoundedCaptureHandoff(kind, GENERATION, buffers, releases, ledger, faults, capacity, LIMIT, lifetime)

        /** A codec buffer at [index] holding [size] recognisable bytes, observed under [pts] unless it is a config or an empty terminal. */
        fun stage(index: Int, size: Int, pts: Long, flags: Int = CaptureContract.FLAG_KEY_FRAME): CodecOutput {
            buffers.put(index, ByteBuffer.wrap(ByteArray(size) { ((index shl 4) + it).toByte() }))
            val media = flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && size > 0
            if (media) assertEquals(ObserveOutcome.Recorded, ledger.observe(GENERATION, kind, CaptureObservation(pts, 1_000L + pts)))
            return CodecOutput(index, 0, size, pts, flags)
        }
    }

    /** The DECLARED refusal for an oversized output: the kind, the offered size and the limit, whole. */
    private fun oversized(kind: MediaKind, size: Int): CaptureFault = CaptureFault.Malformed(kind, "the $kind output of $size bytes exceeds the limit of $LIMIT bytes")

    private fun bytesOf(out: CapturedOutput): ByteArray = out.output.buffer.copyOfRange(out.output.offset, out.output.offset + out.output.size)

    private object RefusingLease : CodecLifetime {
        override fun retainForTransfer(): Boolean = false

        override fun releaseAfterTransfer() = Unit
    }

    // ---- g: this file's oracles, executable now --------------------------------------

    /** The declared fault is compared whole: the other kind, the other size and the other limit are all different faults. */
    @Test
    fun g1_theOversizedFaultOracleIsWhole() {
        assertEquals(CaptureFault.Malformed(MediaKind.Video, "the Video output of 9 bytes exceeds the limit of 8 bytes"), oversized(MediaKind.Video, 9))
        assertTrue(oversized(MediaKind.Video, 9) != oversized(MediaKind.Audio, 9))
        assertTrue(oversized(MediaKind.Video, 9) != oversized(MediaKind.Video, 10))
        assertTrue(oversized(MediaKind.Video, 9) != CaptureFault.Malformed(MediaKind.Video, "the Video output of 9 bytes exceeds the limit of 9 bytes"))
        assertTrue(oversized(MediaKind.Video, 9) != CaptureFault.Overflow(MediaKind.Video, "the Video output of 9 bytes exceeds the limit of 8 bytes"))
    }

    /** The no-access oracles are live: a fetch shows in `fetched`, a claim consumes an observation, a release shows in `indices`. */
    @Test
    fun g2_theAccessInventoriesObserveAFetchAClaimAndARelease() {
        val rig = HandoffRig(MediaKind.Video)
        val out = rig.stage(3, 4, 10L)
        assertEquals(emptyList(), rig.buffers.fetched())
        assertEquals(1, rig.ledger.outstanding)
        assertEquals(emptyList(), rig.releases.indices())
        assertEquals(HandoffOutcome.Queued, rig.handoff.onOutput(GENERATION, out))
        assertEquals(listOf(3), rig.buffers.fetched())
        assertEquals(0, rig.ledger.outstanding, "the admitted media claimed its observation")
        assertEquals(listOf(3), rig.releases.indices())
    }

    // ---- b: the bound, through the real handoff, session route and composition --------

    /** A zero or negative limit is an argument error at construction; nothing is fetched or released to validate it. */
    @Test
    fun b0_aNonPositiveLimitIsAnArgumentErrorAtConstruction() {
        for (limit in listOf(0, -1)) {
            val buffers = HostBuffers()
            val releases = HostReleases()
            assertFailsWith<IllegalArgumentException>("limit $limit") {
                BoundedCaptureHandoff(MediaKind.Video, GENERATION, buffers, releases, HostLedger(MediaKind.Video, GENERATION), HostFaults(), CAPACITY, limit)
            }
            assertEquals(emptyList(), buffers.fetched())
            assertEquals(emptyList(), releases.indices())
        }
    }

    /** Limit-1 and exactly-limit payloads are admitted byte-exact; limit+1 is refused whole, before fetch, claim, copy or reservation. */
    @Test
    fun b1_limitAndBelowAreAdmittedByteExactAndLimitPlusOneIsRefusedBeforeAnyAccess() {
        for (kind in listOf(MediaKind.Video, MediaKind.Audio)) {
            val rig = HandoffRig(kind)
            val under = rig.stage(0, LIMIT - 1, 10L)
            val exact = rig.stage(1, LIMIT, 20L)
            val over = rig.stage(2, LIMIT + 1, 30L)
            assertEquals(HandoffOutcome.Queued, rig.handoff.onOutput(GENERATION, under))
            assertContentEquals(ByteArray(LIMIT - 1) { it.toByte() }, bytesOf(assertNotNull(rig.handoff.next())), "$kind: limit-1 admitted byte-exact")
            assertEquals(HandoffOutcome.Queued, rig.handoff.onOutput(GENERATION, exact))
            assertContentEquals(ByteArray(LIMIT) { (16 + it).toByte() }, bytesOf(assertNotNull(rig.handoff.next())), "$kind: exactly the limit admitted byte-exact")
            assertEquals(1, rig.ledger.outstanding, "$kind: the oversized output's observation is still unclaimed")
            assertIs<HandoffOutcome.Refused>(rig.handoff.onOutput(GENERATION, over), "$kind: limit+1 is refused")
            assertEquals(listOf(oversized(kind, LIMIT + 1)), rig.faults.faults(), "$kind: the declared fault, once")
            assertEquals(listOf(0, 1, 2), rig.releases.indices(), "$kind: the refused index went back exactly once")
            assertEquals(listOf(0, 1), rig.buffers.fetched(), "$kind: the oversized output was never fetched")
            assertEquals(1, rig.ledger.outstanding, "$kind: and its observation was never claimed")
            assertNull(rig.handoff.next(), "$kind: nothing was queued for it")
            assertTrue(rig.handoff.ended, "$kind: the refusal ended the track")
        }
    }

    /** Codec-config and payload+EOS above the limit are refused too; an empty EOS and a config within the limit keep their behavior. */
    @Test
    fun b2_configAndFinalPayloadAboveTheLimitAreRefusedWhileEmptyEosAndSmallConfigAreNot() {
        val config = HandoffRig(MediaKind.Video)
        val small = config.stage(0, LIMIT, 0L, CaptureContract.FLAG_CODEC_CONFIG)
        assertEquals(HandoffOutcome.Queued, config.handoff.onOutput(GENERATION, small))
        assertEquals(0, config.ledger.outstanding, "a configuration claims nothing")
        assertNotNull(config.handoff.next())
        val big = config.stage(1, LIMIT + 1, 0L, CaptureContract.FLAG_CODEC_CONFIG)
        assertIs<HandoffOutcome.Refused>(config.handoff.onOutput(GENERATION, big), "an oversized configuration is refused")
        assertEquals(listOf(oversized(MediaKind.Video, LIMIT + 1)), config.faults.faults())
        assertEquals(listOf(0), config.buffers.fetched(), "never fetched")
        assertEquals(listOf(0, 1), config.releases.indices(), "released once")

        val final = HandoffRig(MediaKind.Audio)
        val terminal = final.stage(0, LIMIT + 1, 10L, CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_END_OF_STREAM)
        assertIs<HandoffOutcome.Refused>(final.handoff.onOutput(GENERATION, terminal), "an oversized final payload is refused")
        assertEquals(listOf(oversized(MediaKind.Audio, LIMIT + 1)), final.faults.faults())
        assertEquals(emptyList(), final.buffers.fetched())
        assertEquals(1, final.ledger.outstanding, "never claimed")
        assertEquals(listOf(0), final.releases.indices())

        val empty = HandoffRig(MediaKind.Audio)
        val eos = empty.stage(0, 0, 0L, CaptureContract.FLAG_END_OF_STREAM)
        assertEquals(HandoffOutcome.Queued, empty.handoff.onOutput(GENERATION, eos), "an empty EOS is not a minimum-size violation")
        assertTrue(assertNotNull(empty.handoff.next()).output.isEndOfStream)
        assertEquals(emptyList(), empty.faults.faults())
    }

    /**
     * The refusal travels the real session route during the format wait: the
     * session fails by that fault, the run settles promptly and unfinished,
     * no track is added, the earlier queued payload is discarded by the failed
     * close with the declared inventory, and the oversized one was never
     * fetched or queued.
     */
    @Test
    fun b3_anOversizedCallbackFailsTheSessionThroughTheRealRouteAndQueuesNothing() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")), maxOutputBytes = LIMIT)
        val feed = CompletableFormats()
        val fault = oversized(MediaKind.Video, LIMIT + 1)
        val outcome =
            withRun({ rig.run(rig.formatPublication(feed.sources(), 2.seconds)) }) { owner ->
                runCurrent()
                stillLive(owner)
                deliver(rig.video, 0, ByteArray(LIMIT - 1) { 1 }, 33_333L)
                rig.video.buffers.put(1, ByteBuffer.wrap(ByteArray(LIMIT + 1) { 2 }))
                assertEquals(ObserveOutcome.Recorded, rig.video.ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(66_666L, BASE_NANOS + 66_666_000L)))
                assertIs<HandoffOutcome.Refused>(rig.video.handoff.onOutput(GENERATION, CodecOutput(1, 0, LIMIT + 1, 66_666L, CaptureContract.FLAG_KEY_FRAME)))
                runCurrent()
                assertTrue(owner.reached, "settled promptly at $currentTime ms")
                val result = owner.observed()
                assertEquals(0L, currentTime)
                result
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome.getOrThrow())
        assertEquals(CaptureTermination.Failed(fault), rig.events.terminal.getCompleted())
        assertEquals("the capture session ended before the tracks were prepared: ${CaptureTermination.Failed(fault)}", unfinished.reason)
        expectEffects(emptyList(), rig.tracks.all())
        assertTrue(rig.owner.isDecided)
        assertEquals(listOf(0), rig.video.buffers.fetched(), "the oversized callback was never fetched")
        assertEquals(listOf(0, 1), rig.video.releases.indices(), "both indices released exactly once")
        assertEquals(emptyList(), rig.audio.releases.indices())
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), rig.forward.discards(), "exactly the one earlier payload was discarded by the failed close")
    }

    /** The composition validates the limit before any acquisition, and forwards an explicitly small limit to BOTH real handoffs. */
    @Test
    fun b4_theCompositionValidatesAndForwardsTheLimitToBothHandoffs() {
        for (limit in listOf(0, -1)) {
            val env = Environment()
            assertFailsWith<IllegalArgumentException>("limit $limit") { env.compose(limit) }
            assertEquals(0, env.acquirer.begins, "no acquisition was attempted")
            assertEquals(emptyList(), env.videoBuffers.fetched())
            assertEquals(emptyList(), env.audioBuffers.fetched())
        }
        for (kind in listOf(MediaKind.Video, MediaKind.Audio)) {
            val env = Environment()
            val capture = env.compose(LIMIT)
            val inputs = if (kind == MediaKind.Video) capture.video else capture.audio
            val buffers = if (kind == MediaKind.Video) env.videoBuffers else env.audioBuffers
            val releases = if (kind == MediaKind.Video) env.videoIndices else env.audioIndices
            buffers.put(0, ByteBuffer.wrap(ByteArray(LIMIT + 1) { 3 }))
            assertEquals(ObserveOutcome.Recorded, inputs.ledger.observe(GENERATION, kind, CaptureObservation(10L, BASE_NANOS + 10_000L)))
            assertIs<HandoffOutcome.Refused>(inputs.handoff.onOutput(GENERATION, CodecOutput(0, 0, LIMIT + 1, 10L, CaptureContract.FLAG_KEY_FRAME)), "$kind: the composed handoff carries the limit")
            assertEquals(emptyList(), buffers.fetched(), "$kind: never fetched")
            assertEquals(listOf(0), releases.indices(), "$kind: released once")
            assertEquals(1, inputs.ledger.outstanding, "$kind: never claimed")
        }
    }

    /** Existing precedence is kept: stale generation and a closed track are ignored, a reordered or overflowing oversized output is refused by ITS fault. */
    @Test
    fun b5_staleClosedReorderedAndFullPrecedenceIsPreserved() {
        val stale = HandoffRig(MediaKind.Video)
        assertIs<HandoffOutcome.Ignored>(stale.handoff.onOutput(GENERATION + 1, stale.stage(0, LIMIT + 1, 10L)))
        assertEquals(emptyList(), stale.faults.faults())
        assertEquals(emptyList(), stale.releases.indices())
        assertEquals(emptyList(), stale.buffers.fetched())

        val closed = HandoffRig(MediaKind.Video)
        closed.handoff.close(CloseMode.Completed)
        assertIs<HandoffOutcome.Ignored>(closed.handoff.onOutput(GENERATION, closed.stage(0, LIMIT + 1, 10L)))
        assertEquals(emptyList(), closed.faults.faults())
        assertEquals(emptyList(), closed.releases.indices())

        val reordered = HandoffRig(MediaKind.Video)
        assertEquals(HandoffOutcome.Queued, reordered.handoff.onOutput(GENERATION, reordered.stage(0, 4, 20L)))
        assertIs<HandoffOutcome.Refused>(reordered.handoff.onOutput(GENERATION, reordered.stage(1, LIMIT + 1, 10L)))
        assertIs<CaptureFault.Reordered>(reordered.faults.faults().single(), "reorder precedes the size check")
        assertEquals(listOf(0, 1), reordered.releases.indices())
        assertEquals(listOf(0), reordered.buffers.fetched())

        val full = HandoffRig(MediaKind.Audio, capacity = 1)
        assertEquals(HandoffOutcome.Queued, full.handoff.onOutput(GENERATION, full.stage(0, 4, 10L)))
        assertIs<HandoffOutcome.Refused>(full.handoff.onOutput(GENERATION, full.stage(1, LIMIT + 1, 20L)))
        assertIs<CaptureFault.Overflow>(full.faults.faults().single(), "a full queue precedes the size check")
        assertEquals(listOf(0, 1), full.releases.indices())
        assertEquals(listOf(0), full.buffers.fetched())
    }

    /** A refused lease keeps the no-codec-access rule on the size refusal: the declared fault is reported and no index is released or fetched. */
    @Test
    fun b6_aRefusedLeaseReportsTheSizeFaultWithoutTouchingTheCodec() {
        val rig = HandoffRig(MediaKind.Video, lifetime = RefusingLease)
        assertIs<HandoffOutcome.Refused>(rig.handoff.onOutput(GENERATION, rig.stage(0, LIMIT + 1, 10L)))
        assertEquals(listOf(oversized(MediaKind.Video, LIMIT + 1)), rig.faults.faults(), "the size fault, not a lease/codec fault: it is decided before any lease")
        assertEquals(emptyList(), rig.releases.indices(), "no release without a lease")
        assertEquals(emptyList(), rig.buffers.fetched())
    }

    /** An index owner whose return of [failing] raises the DECLARED instance. */
    private class DeclaredReleases(private val failing: Int, private val raise: Throwable) : CodecBufferOwner {
        private val log = mutableListOf<Int>()

        @Synchronized
        override fun release(index: Int) {
            log.add(index)
            if (index == failing) throw raise
        }

        @Synchronized
        fun indices(): List<Int> = log.toList()
    }

    /**
     * The declared refusal-cleanup contract, independent of which refusal:
     * the index return's failure leaves by identity, the refusal's fault is
     * still reported exactly once, the index was attempted once, the refused
     * callback was never fetched or claimed, and the track has ended.
     */
    private fun refusalWithFailingIndexReturn(capacity: Int, secondSize: Int, expectedFault: (MediaKind) -> CaptureFault) {
        val declared = IllegalStateException("the index return failed")
        val buffers = HostBuffers()
        val releases = DeclaredReleases(1, declared)
        val faults = HostFaults()
        val ledger = HostLedger(MediaKind.Video, GENERATION)
        val handoff = BoundedCaptureHandoff(MediaKind.Video, GENERATION, buffers, releases, ledger, faults, capacity, LIMIT)
        buffers.put(0, ByteBuffer.wrap(ByteArray(4)))
        ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(10L, 1_010L))
        assertEquals(HandoffOutcome.Queued, handoff.onOutput(GENERATION, CodecOutput(0, 0, 4, 10L, CaptureContract.FLAG_KEY_FRAME)))
        buffers.put(1, ByteBuffer.wrap(ByteArray(secondSize)))
        ledger.observe(GENERATION, MediaKind.Video, CaptureObservation(20L, 1_020L))
        val result = runCatching { handoff.onOutput(GENERATION, CodecOutput(1, 0, secondSize, 20L, CaptureContract.FLAG_KEY_FRAME)) }
        val thrown = result.exceptionOrNull() ?: throw AssertionError("expected the declared index-return failure to leave, but the refusal returned ${result.getOrNull()}")
        if (thrown !== declared) throw AssertionError("expected the declared index-return failure to leave by identity, but $thrown left", thrown)
        assertEquals(listOf(expectedFault(MediaKind.Video)), faults.faults(), "the refusal's own fault, reported once; the index failure is not relabelled")
        assertEquals(listOf(0, 1), releases.indices(), "the refused index attempted exactly once")
        assertEquals(listOf(0), buffers.fetched(), "the refused callback was never fetched")
        assertEquals(1, ledger.outstanding, "nor claimed")
        assertTrue(handoff.ended)
    }

    /** The accepted OVERFLOW refusal under the declared cleanup contract. Independent of the cap. */
    @Test
    fun b7_anIndexReturnFailureAtAnOverflowRefusalLeavesByIdentity() {
        refusalWithFailingIndexReturn(capacity = 1, secondSize = 4) { kind -> CaptureFault.Overflow(kind, "the $kind queue holds 1 outputs") }
    }

    /** The SIZE refusal under the same declared contract. */
    @Test
    fun b8_anIndexReturnFailureAtASizeRefusalLeavesByIdentity() {
        refusalWithFailingIndexReturn(capacity = CAPACITY, secondSize = LIMIT + 1) { kind -> oversized(kind, LIMIT + 1) }
    }

    // ---- the minimal real composition -------------------------------------------------

    private class RecordingAcquirer : CaptureAcquirer {
        var begins = 0
        override val order: List<CaptureResource> = emptyList()

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
            begins++
            return null
        }
    }

    private class NoSink : TrackSink {
        override suspend fun write(obj: SendObject, timeout: Duration): WriteOutcome = WriteOutcome.Accepted

        override suspend fun endTrack(timeout: Duration): WriteOutcome = WriteOutcome.Accepted
    }

    private class Environment {
        val acquirer = RecordingAcquirer()
        val videoBuffers = HostBuffers()
        val videoIndices = HostReleases()
        val audioBuffers = HostBuffers()
        val audioIndices = HostReleases()

        fun compose(limit: Int): LiveCapture =
            composeLiveBroadcast(
                generation = GENERATION,
                permissions = Granted(GENERATION),
                cameraFacts = CaptureCapability.Available,
                audioFacts = CaptureCapability.Available,
                calibration = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = UnixMicros(EPOCH), uncertaintyMicros = 10L),
                origin = ORIGIN,
                audioKeying = HostAudioKeying(48_000, 1_024, ORIGIN),
                capacity = CAPACITY,
                maxOutputBytes = limit,
                maxNalUnits = HOST_MAX_NAL_UNITS,
                video = validatedVideo(),
                audio = validatedAudio(),
                environment =
                    CaptureEnvironment(
                        acquirer = acquirer,
                        dispatcher = HostDispatcher(),
                        late = LateLog(),
                        video = TrackCodec(CodecLifetime.RETAINED, videoBuffers, videoIndices),
                        audio = TrackCodec(CodecLifetime.RETAINED, audioBuffers, audioIndices),
                    ),
                sinks = PublishSinks(NoSink(), NoSink(), Completion()),
                record = CaptureDiscardRecord(GENERATION),
                writeTimeout = WRITE_TIMEOUT,
                drainTimeout = DRAIN_TIMEOUT,
            )
    }

    /** The two format sources of a wait that never resolves: the session route row needs a live preparation, not formats. */
    private class CompletableFormats {
        val video = kotlinx.coroutines.CompletableDeferred<CompleteTrackFormat>()
        val audio = kotlinx.coroutines.CompletableDeferred<CompleteTrackFormat>()

        fun sources() = FormatSources(video, audio)
    }

    private companion object {
        /** Small and explicit: every fixture payload in this file is sized against it. */
        const val LIMIT = 8
    }
}
