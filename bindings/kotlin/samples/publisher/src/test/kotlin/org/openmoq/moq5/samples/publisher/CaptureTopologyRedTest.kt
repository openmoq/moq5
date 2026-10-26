package org.openmoq.moq5.samples.publisher

import java.io.IOException
import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The sample-facing capture topology: one session, one generation, two tracks,
 * driven against the ACCEPTED `PublishPipeline`.
 *
 * The ownership and both handoffs are the conforming host fixtures, and
 * acquisition is a SCRIPTED driver with a declared order and real,
 * identity-bearing handles -- so a partial failure has a nonempty, independently
 * expected prefix, and every close is observable by identity. Every failure
 * here is the SESSION's own boundary.
 *
 * Nothing here acquires a camera or a microphone.
 */
class CaptureTopologyRedTest {

    /**
     * A scripted platform, with the platform's own dependency: the capture
     * session is submitted only once the camera has OPENED, because
     * `createCaptureSession` is called on the `CameraDevice` that `onOpened`
     * hands over. So the camera is the only step in flight when `begin`
     * returns, and the stream's ticket appears from inside `onOpened`.
     */
    private class ScriptedAcquirer(
        private val platform: Platform,
        private val failAt: CaptureResource? = null,
        private val failSubmission: CaptureResource? = null,
        /** What the driver's own submission throws where it stands, and where. */
        private val raiseAt: CaptureResource? = null,
        private val raise: (() -> Throwable)? = null,
        /** Runs once inside begin, after every step has been submitted. */
        private val duringBegin: (() -> Unit)? = null,
    ) : CaptureAcquirer {
        override val order =
            listOf(
                CaptureResource.Worker, CaptureResource.VideoEncoder, CaptureResource.CameraSurface,
                CaptureResource.Camera, CaptureResource.Microphone, CaptureResource.AudioEncoder,
            )

        /** What each settled step submits next, once it has actually settled. */
        private val dependent = mapOf(CaptureResource.Camera to CaptureResource.CameraStream)

        val tickets = mutableMapOf<CaptureResource, AcquisitionTicket>()
        private var steps: AcquisitionSteps? = null
        var beginCalls = 0
            private set

        override fun begin(ownership: CaptureOwnership, steps: AcquisitionSteps): AcquireOutcome.Failed? {
            beginCalls++
            this.steps = steps
            for (resource in order) {
                if (resource == failAt) return AcquireOutcome.Failed(resource, "$resource refused")
                if (resource == raiseAt) throw checkNotNull(raise).invoke()
                when (resource) {
                    CaptureResource.Camera -> submit(ownership, resource)
                    else -> {
                        val outcome = ownership.acquire(resource) { platform.handle(resource) }
                        if (outcome is AcquireOutcome.Failed) return outcome
                    }
                }
            }
            duringBegin?.invoke()
            return null
        }

        /** The platform calls back: onOpened / onConfigured, then the driver reports the settlement. */
        fun settle(ownership: CaptureOwnership, resource: CaptureResource): AdoptOutcome {
            val ticket = tickets[resource] ?: return AdoptOutcome.Unknown("nothing was requested for $resource")
            val outcome = ownership.adopt(ticket, platform.handle(resource))
            // The dependent step is SUBMITTED before this settlement is
            // reported, so the outstanding count never dips through zero.
            var submissionFailure: AdoptOutcome.Failed? = null
            if (outcome == AdoptOutcome.Adopted) {
                dependent[resource]?.let { next ->
                    val ticketForNext = submit(ownership, next)
                    if (next == failSubmission) {
                        // createCaptureSession threw where it stands, inside onOpened.
                        submissionFailure =
                            ownership.fail(ticketForNext, "$next could not be submitted", null) as? AdoptOutcome.Failed
                    }
                }
            }
            steps?.settled(resource, outcome)
            submissionFailure?.let { steps?.settled(dependent.getValue(resource), it) }
            return outcome
        }

        private fun submit(ownership: CaptureOwnership, resource: CaptureResource): AcquisitionTicket =
            ownership.request(resource).also {
                tickets[resource] = it
                steps?.submitted(resource)
            }
    }

    private class Platform(private val reportOnClose: Map<CaptureResource, Exception> = emptyMap()) {
        val closes = mutableListOf<Any>()
        private val resources = CaptureResource.entries.associateWith { HostResource(it.name.lowercase()) }

        fun identity(resource: CaptureResource): HostResource = resources.getValue(resource)

        /** A close that REPORTS a failure -- the ownership returns it rather than throwing it. */
        fun handle(resource: CaptureResource) = HostHandle(identity(resource), closes, reportOnClose[resource])

        fun closed(): List<Any> = synchronized(closes) { closes.toList() }
    }

    private class Track(val kind: MediaKind, sink: CaptureFaultSink) {
        val buffers = HostBuffers()
        val releases = HostReleases()
        val ledger = HostLedger(kind, GENERATION, capacity = 16)
        val handoff = ConformingHandoff(kind, GENERATION, buffers, releases, ledger, sink, capacity = 4)

        fun output(index: Int, bytes: ByteArray, pts: Long, captureNanos: Long, flags: Int = CaptureContract.FLAG_KEY_FRAME): CodecOutput {
            buffers.put(index, ByteBuffer.wrap(bytes))
            if (flags and CaptureContract.FLAG_CODEC_CONFIG == 0 && bytes.isNotEmpty()) ledger.observe(GENERATION, kind, CaptureObservation(pts, captureNanos))
            return CodecOutput(index, 0, bytes.size, pts, flags)
        }
    }

    /** Forwards each track's faults into whichever session is wired after construction. */
    private class Forward : CaptureFaultSink {
        var to: CaptureFaultSink? = null

        override fun onFault(generation: Long, fault: CaptureFault) {
            to?.onFault(generation, fault)
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) {
            to?.onDiscarded(generation, kind, outputs)
        }
    }

    /** A track that closes, and then raises what a real close may raise. */
    private class RaisingClose(private val inner: CaptureHandoff, private val raise: () -> Throwable) : CaptureHandoff by inner {
        override fun close(mode: CloseMode) {
            inner.close(mode)
            throw raise()
        }
    }

    /** Records what the session says, and may fail the way application code does. */
    private class RecordingListener(private val raiseOnTerminated: (() -> Throwable)? = null) : CaptureSessionListener {
        private val events = mutableListOf<Any>()

        @Synchronized
        override fun onReady(generation: Long, ready: CaptureReady) {
            events.add(ready)
        }

        @Synchronized
        override fun onTerminated(generation: Long, termination: CaptureTermination) {
            events.add(termination)
            raiseOnTerminated?.let { throw it() }
        }

        @Synchronized
        fun all(): List<Any> = events.toList()
    }

    private class Rig(
        failAt: CaptureResource? = null,
        videoCloseRaises: (() -> Throwable)? = null,
        raiseAt: CaptureResource? = null,
        raise: (() -> Throwable)? = null,
        duringBegin: (() -> Unit)? = null,
        terminationRaises: (() -> Throwable)? = null,
        reportOnClose: Map<CaptureResource, Exception> = emptyMap(),
    ) {
        val platform = Platform(reportOnClose)
        val late = LateLog()
        val ownership = ConformingOwnership(GENERATION, HostDispatcher(), late)
        val acquirer = ScriptedAcquirer(platform, failAt, raiseAt = raiseAt, raise = raise, duringBegin = duringBegin)
        val forward = Forward()
        val video = Track(MediaKind.Video, forward)
        val audio = Track(MediaKind.Audio, forward)

        /** What the session is GIVEN for the video track: the track itself, unless a close must throw. */
        val videoTrack: CaptureHandoff = videoCloseRaises?.let { RaisingClose(video.handoff, it) } ?: video.handoff
        val listener = RecordingListener(terminationRaises)

        fun wire(session: CaptureSession): CaptureSession = session.also { forward.to = it }

        /** Start, then settle the two asynchronous steps as the platform would. */
        fun startAndSettle(session: CaptureSession): CaptureStart {
            val start = session.start(GrantedFlow(GENERATION), CaptureCapability.Available, CaptureCapability.Available)
            if (start == CaptureStart.Pending) {
                ownership.dispatcher.deliver { acquirer.settle(ownership, CaptureResource.Camera) }
                ownership.dispatcher.deliver { acquirer.settle(ownership, CaptureResource.CameraStream) }
            }
            return start
        }
    }

    private fun interface Make {
        fun make(rig: Rig): CaptureSession
    }

    private class GrantedFlow(override val generation: Long) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.Granted

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored("granted")

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Ready
    }

    private class RefusedFlow(override val generation: Long, private val why: String) : PermissionFlow {
        override fun stateOf(permission: CapturePermission) = PermissionState.PermanentlyDenied

        override fun request(permissions: Set<CapturePermission>) = emptySet<CapturePermission>()

        override fun onResult(generation: Long, permission: CapturePermission, facts: PermissionFacts) = PermissionOutcome.Ignored(why)

        override fun retire(reason: String) = Unit

        override fun readiness(): CaptureReadiness = CaptureReadiness.Refused(why)
    }

    private val available = CaptureCapability.Available
    private val unusable = CaptureCapability.Unavailable("the camera stamps on an unknown timebase")

    // ---- the contract rows ----------------------------------------------------

    private fun t1_permissionsAndBothCapabilityFactsGateAcquisition(make: Make) {
        for ((flow, cam, aud) in listOf(
            Triple(RefusedFlow(GENERATION, "camera permanently denied"), available, available),
            Triple(GrantedFlow(GENERATION), unusable, available),
            Triple(GrantedFlow(GENERATION), available, unusable),
        )) {
            val rig = Rig()
            val session = rig.wire(make.make(rig))
            assertIs<CaptureStart.Refused>(session.start(flow, cam, aud))
            assertEquals(emptyList(), rig.platform.closed(), "a refusal acquired nothing, so nothing closes")
            assertEquals(0, rig.ownership.pending, "and requested nothing")
            assertNull(session.video)
            assertNull(session.audio)
        }
    }

    private fun t2_startIsPendingUntilTheAsynchronousStepsSettleThenReady(make: Make) {
        val rig = Rig()
        val session = rig.wire(make.make(rig))
        val start = session.start(GrantedFlow(GENERATION), available, available)
        assertEquals(CaptureStart.Pending, start, "openCamera settles by callback: start is accepted, not ready")
        assertEquals(1, rig.ownership.pending, "only the camera is in flight: createCaptureSession needs the device onOpened hands over")
        assertNull(rig.acquirer.tickets[CaptureResource.CameraStream], "so the stream cannot have been requested yet")
        assertEquals(emptyList(), rig.listener.all(), "readiness is not claimed before the platform has called back")
        rig.ownership.dispatcher.deliver { assertEquals(AdoptOutcome.Adopted, rig.acquirer.settle(rig.ownership, CaptureResource.Camera)) }
        assertEquals(1, rig.ownership.pending, "onOpened submitted the capture session before it reported the camera")
        assertEquals(emptyList(), rig.listener.all(), "an opened camera is not readiness: the step it enables is now in flight")
        rig.ownership.dispatcher.deliver { assertEquals(AdoptOutcome.Adopted, rig.acquirer.settle(rig.ownership, CaptureResource.CameraStream)) }
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.listener.all(), "ready arrives through the session's own route, once, when both have settled")
        assertSame(rig.video.handoff, session.video, "the session publishes through the handoff it was given")
        assertSame(rig.audio.handoff, session.audio)
    }

    private fun t3_aPartialAcquisitionUnwindsTheExpectedPrefix(make: Make) {
        val rig = Rig(failAt = CaptureResource.AudioEncoder)
        val session = rig.wire(make.make(rig))
        assertIs<CaptureStart.Refused>(rig.startAndSettle(session), "a synchronous step failed: refused by name")
        // Independently expected: everything BEFORE the failing step, closed in
        // reverse, by identity -- with the two callback-settled steps still
        // outstanding and therefore NOT in this list, and the worker retained
        // for them.
        val p = rig.platform
        assertEquals(
            listOf<Any>(p.identity(CaptureResource.Microphone), p.identity(CaptureResource.CameraSurface), p.identity(CaptureResource.VideoEncoder)),
            p.closed(),
            "the nonempty prefix unwinds in reverse; the worker waits for the callback still in flight",
        )
        assertEquals(1, rig.ownership.pending, "the camera was submitted before the failing step and is still opening")
        assertTrue(rig.ownership.isClosed)
        // The platform still calls back. The camera it carries is closed late,
        // the worker goes last, and the step that camera would have enabled is
        // never submitted, because it never opened for this session.
        rig.ownership.dispatcher.deliver { assertIs<AdoptOutcome.Retired>(rig.acquirer.settle(rig.ownership, CaptureResource.Camera)) }
        assertNull(rig.acquirer.tickets[CaptureResource.CameraStream], "a retired camera enables nothing")
        assertEquals(
            listOf<Any>(
                p.identity(CaptureResource.Microphone), p.identity(CaptureResource.CameraSurface), p.identity(CaptureResource.VideoEncoder),
                p.identity(CaptureResource.Camera), p.identity(CaptureResource.Worker),
            ),
            p.closed(),
        )
        assertEquals(0, rig.ownership.pending, "nothing is left outstanding")
        assertNull(session.video)
    }

    private fun t4_bothTracksFeedTheAcceptedPipelineThroughIdleOutputIdleAndEos(make: Make) {
        val rig = Rig()
        val session = rig.wire(make.make(rig))
        assertEquals(CaptureStart.Pending, rig.startAndSettle(session))
        val video = assertIs<EncodedOutputSource>(session.video)
        val audio = assertIs<EncodedOutputSource>(session.audio)
        val clock = PlatformCaptureClock(ClockCalibration(elapsedRealtimeNanos = 1_000_000_000L, epoch = UnixMicros(1_700_000_000_000_000uL), uncertaintyMicros = 10L))
        val timeline = PublishTimeline(0L)
        val vPipe = PublishPipeline(MediaKind.Video, timeline, clock, validatedVideo(), HOST_LIMITS)
        val aPipe = PublishPipeline(MediaKind.Audio, timeline, clock, validatedAudio(), HOST_LIMITS)
        val signals = mutableListOf<MediaKind>()
        rig.video.handoff.signal { signals.add(it) }
        rig.audio.handoff.signal { signals.add(it) }

        // IDLE: a live track is empty and NOT ended; the frozen `?: break` would
        // have ended the broadcast right here.
        assertNull(video.next())
        assertTrue(!rig.video.handoff.ended, "empty is not ended")
        assertNull(audio.next())
        assertTrue(!rig.audio.handoff.ended)
        assertNull(session.termination, "and nothing completed")

        // One real video output, then one real audio output, each through the
        // ACCEPTED pipeline, with exact bytes and exact capture instants.
        val vBytes = VIDEO_ANNEXB.copyOf()
        assertEquals(HandoffOutcome.Queued, rig.video.handoff.onOutput(GENERATION, rig.video.output(0, vBytes, pts = 33_333L, captureNanos = 1_000_500_000L)))
        assertEquals(listOf(MediaKind.Video), signals, "the consumer is signalled, not polled")
        // Drained ONCE, and BOTH of the item's fields are used: the encoded
        // output goes to the pipeline, and the instant the pipeline is given is
        // the one the item carries -- never a literal repeated from staging.
        val vItem = assertNotNull(video.next())
        val vWrite = videoWrite(vPipe, vItem, VIDEO_AVC1)
        assertNull(video.next(), "idle again")

        val aBytes = ByteArray(3) { (0x20 + it).toByte() }
        assertEquals(HandoffOutcome.Queued, rig.audio.handoff.onOutput(GENERATION, rig.audio.output(0, aBytes, pts = 21_333L, captureNanos = 1_000_250_000L)))
        val aItem = assertNotNull(audio.next())
        val aWrite = audioWrite(aPipe, aItem, aBytes)
        vPipe.accepted(vWrite.obj)
        aPipe.accepted(aWrite.obj)

        // Per-track EOS: the pipeline's EndTrack is that TRACK's end; the
        // session completes only after BOTH.
        rig.video.handoff.onOutput(GENERATION, rig.video.output(1, ByteArray(0), pts = 0L, captureNanos = 0L, flags = CaptureContract.FLAG_END_OF_STREAM))
        assertEquals(PublishStep.EndTrack, vPipe.step(video.next()!!.output, 0L))
        assertTrue(rig.video.handoff.ended)
        session.onTrackEnded(GENERATION, MediaKind.Video)
        assertNull(session.termination, "one track ending is not the broadcast ending")
        rig.audio.handoff.onOutput(GENERATION, rig.audio.output(1, ByteArray(0), pts = 0L, captureNanos = 0L, flags = CaptureContract.FLAG_END_OF_STREAM))
        assertEquals(PublishStep.EndTrack, aPipe.step(audio.next()!!.output, 0L))
        session.onTrackEnded(GENERATION, MediaKind.Audio)
        assertEquals(CaptureTermination.Completed, session.termination, "both declared endings reached: completed")
        assertEquals(listOf<Any>(CaptureReady.Ready, CaptureTermination.Completed), rig.listener.all())
        assertTrue(rig.ownership.isClosed)
    }

    private fun t5_theFirstFaultArrivesThroughTheProductionRouteAndTerminatesOnce(make: Make) {
        val rig = Rig()
        val session = rig.wire(make.make(rig))
        rig.startAndSettle(session)
        rig.video.handoff.onOutput(GENERATION, rig.video.output(0, ByteArray(4), 0L, 1L))
        // The codec fails: the track reports through its sink -- the session --
        // not through a direct test call.
        rig.video.handoff.onCodecError(GENERATION, "CodecException: video")
        rig.audio.handoff.onCodecError(GENERATION, "CodecException: audio, a moment later")
        val ended = assertIs<CaptureTermination.Failed>(session.termination)
        assertEquals(CaptureFault.Codec(MediaKind.Video, "CodecException: video"), ended.fault, "the first fault is the cause; the second does not rewrite it")
        assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "terminated once")
        assertTrue(rig.video.handoff.ended && rig.audio.handoff.ended, "both tracks closed")
        assertTrue(rig.ownership.isClosed)
        // Ownership order is ADOPTION order: the two callback-settled steps were
        // installed when the platform called back, after every synchronous step,
        // so they close first -- stream before the camera it was configured on.
        val installed =
            rig.acquirer.order.filterNot { it == CaptureResource.Camera || it == CaptureResource.CameraStream } +
                listOf(CaptureResource.Camera, CaptureResource.CameraStream)
        assertEquals(installed.reversed().map { rig.platform.identity(it) }, rig.platform.closed(), "every acquired resource closed once, in reverse adoption order")
    }

    private fun t6_everyEndingConvergesOnOneIdempotentStop(make: Make) {
        val rig = Rig()
        val session = rig.wire(make.make(rig))
        rig.startAndSettle(session)
        val primary = IllegalStateException("the broadcast failed")
        assertSame(primary, session.stop(primary))
        assertNull(session.stop(null), "stopping twice stops nothing twice")
        assertEquals(1, rig.listener.all().count { it is CaptureTermination })
        assertEquals(
            rig.acquirer.order.size + 1,
            rig.platform.closed().size,
            "each resource closed exactly once: every declared step, plus the one the opened camera enabled",
        )
        val late = CaptureFault.Codec(MediaKind.Video, "a callback that lost the race")
        session.onFault(GENERATION, late)
        assertTrue((session.termination as CaptureTermination.Cancelled).reason == "the broadcast failed", "a fault after the end does not re-end the session")
        assertIs<HandoffOutcome.Ignored>(rig.video.handoff.onOutput(GENERATION, rig.video.output(5, ByteArray(4), 9L, 9L)), "and a late output publishes nothing")
    }

    /**
     * The one place a video write is judged. The elapsed instant handed to the
     * pipeline is the item's own; the epoch expected of it is declared here,
     * from the calibration, independently of what staging filed.
     */
    private fun videoWrite(pipe: PublishPipeline, item: CapturedOutput, expected: ByteArray): PublishStep.Write {
        val write = assertIs<PublishStep.Write>(pipe.step(item.output, item.captureElapsedNanos))
        assertContentEquals(expected, write.obj.payload.bytes(), "the exact bytes reach the write")
        assertEquals(33_333uL, write.obj.presentationTime.value, "on the shared timeline")
        assertEquals(UnixMicros(EPOCH_BASE + 500uL), write.obj.captureTime, CAPTURE_INSTANT)
        return write
    }

    /** The same judgement for audio, with its own track order and its own instant. */
    private fun audioWrite(pipe: PublishPipeline, item: CapturedOutput, expected: ByteArray): PublishStep.Write {
        val write = assertIs<PublishStep.Write>(pipe.step(item.output, item.captureElapsedNanos))
        assertContentEquals(expected, write.obj.payload.bytes())
        assertEquals(21_333uL, write.obj.presentationTime.value, "audio has its own track order; it is not constrained by the later video output")
        assertEquals(UnixMicros(EPOCH_BASE + 250uL), write.obj.captureTime, CAPTURE_INSTANT)
        return write
    }

    /** A track's completion is that TRACK's; the same one twice is still one. */
    private fun t7_theSameTrackEndingTwiceAndAForeignEndingCompleteNothing(make: Make) {
        val rig = Rig()
        val session = rig.wire(make.make(rig))
        rig.startAndSettle(session)
        session.onTrackEnded(GENERATION, MediaKind.Video)
        session.onTrackEnded(GENERATION, MediaKind.Video)
        assertNull(session.termination, "the same track ending twice is one track, not both")
        session.onTrackEnded(GENERATION + 1, MediaKind.Audio)
        assertNull(session.termination, "and an ending stamped for another generation is not this session's")
        session.onTrackEnded(GENERATION, MediaKind.Audio)
        assertEquals(CaptureTermination.Completed, session.termination, "both distinct tracks ended: completed")
        assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "said once")
    }

    /** Start is claimed once: a second start, and a start after the ending, acquire nothing. */
    private fun t8_startIsClaimedOnceAndNeverAfterTheEnding(make: Make) {
        val rig = Rig()
        val session = rig.wire(make.make(rig))
        assertEquals(CaptureStart.Pending, session.start(GrantedFlow(GENERATION), available, available))
        val pending = rig.ownership.pending
        val closedSoFar = rig.platform.closed()
        val again = assertIs<CaptureStart.Refused>(session.start(GrantedFlow(GENERATION), available, available), "a second start is refused")
        assertTrue(again.reason.contains("already been started"), "by name: ${again.reason}")
        assertEquals(1, rig.acquirer.beginCalls, "and the driver was entered exactly once")
        assertEquals(pending, rig.ownership.pending, "nothing else was submitted")
        assertEquals(closedSoFar, rig.platform.closed(), "and nothing was acquired or unwound by it")
        session.stop(null)
        val afterEnd = assertIs<CaptureStart.Refused>(session.start(GrantedFlow(GENERATION), available, available), "a start after the ending is refused")
        assertTrue(afterEnd.reason.contains("already ended"), "by name: ${afterEnd.reason}")
        assertEquals(1, rig.acquirer.beginCalls, "the generation was not resurrected")
        assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "and it ended once")
    }

    /** An earlier close that throws does not strand the closes after it. */
    private fun t9_everyMandatoryCloseIsAttemptedWhenAnEarlierOneThrows(make: Make) {
        val raised = IllegalStateException("the video track's close threw")
        val rig = Rig(videoCloseRaises = { raised })
        val session = rig.wire(make.make(rig))
        rig.startAndSettle(session)
        val primary = IllegalStateException("the broadcast failed")
        val ending = runCatching { session.stop(primary) }
        assertTrue(ending.isSuccess, "a cleanup failure must not replace the ending: ${ending.exceptionOrNull()}")
        assertSame(primary, ending.getOrNull(), "the primary leaves by identity")
        assertTrue(primary.suppressed.any { it === raised }, "carrying the close failure it found")
        assertTrue(rig.audio.handoff.ended, "the second track was closed even though the first threw")
        assertTrue(rig.ownership.isClosed, "and the ownership after it")
        assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "the ending is still announced, once")

        // With no primary to carry it, the cleanup failure escapes rather than
        // disappearing -- after every mandatory close and the announcement.
        val second = IllegalStateException("the video track's close threw again")
        val quiet = Rig(videoCloseRaises = { second })
        val other = quiet.wire(make.make(quiet))
        quiet.startAndSettle(other)
        assertSame(second, assertFailsWith<IllegalStateException> { other.stop(null) })
        assertTrue(quiet.audio.handoff.ended && quiet.ownership.isClosed, "with the rest of the cleanup done")
        assertEquals(1, quiet.listener.all().count { it is CaptureTermination })
    }

    /**
     * The driver throws after a real resource has been adopted. Whatever it
     * throws -- checked, unchecked, Error or cancellation -- is what leaves,
     * and the startup still unwinds completely.
     */
    private fun t10_aDriverThatThrowsAfterAdoptingUnwindsAndKeepsItsFailure(make: Make) {
        val raisers: List<() -> Throwable> =
            listOf(
                { IOException("CameraAccessException: the camera service is unreachable") },
                { AssertionError("an Error raised inside begin") },
                { IllegalArgumentException("the encoder was configured wrongly") },
                { CancellationException("the owner went away mid-acquisition") },
            )
        for (raiser in raisers) {
            val raised = raiser()
            val rig = Rig(raiseAt = CaptureResource.CameraSurface, raise = { raised })
            val session = rig.wire(make.make(rig))
            val outcome = runCatching { session.start(GrantedFlow(GENERATION), available, available) }
            assertSame(raised, outcome.exceptionOrNull(), "what the driver threw is what leaves, by identity")
            val p = rig.platform
            assertEquals(
                listOf<Any>(p.identity(CaptureResource.VideoEncoder), p.identity(CaptureResource.Worker)),
                p.closed(),
                "the resources adopted before it are closed, in reverse",
            )
            assertTrue(rig.ownership.isClosed, "the ownership is not left open")
            assertEquals(0, rig.ownership.pending)
            assertTrue(rig.video.handoff.ended && rig.audio.handoff.ended, "and both tracks were closed")
            assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "one ending, announced once")
            assertEquals(0, rig.listener.all().count { it is CaptureReady }, "and no readiness was claimed")
            assertNull(session.video, "an unsuccessful startup publishes no tracks")
        }
    }

    /** A cleanup failure during that unwind is CARRIED by the original, never substituted for it. */
    private fun t11_aCleanupFailureDuringTheStartupUnwindIsCarriedNotSubstituted(make: Make) {
        val raisers: List<() -> Throwable> =
            listOf(
                { IOException("CameraAccessException: the camera service is unreachable") },
                { AssertionError("an Error raised inside begin") },
                { IllegalArgumentException("the encoder was configured wrongly") },
                { CancellationException("the owner went away mid-acquisition") },
            )
        for (raiser in raisers) {
            val raised = raiser()
            val closeFailure = IllegalStateException("the video track's close threw")
            val listenerFailure = IllegalStateException("the listener threw")
            val rig =
                Rig(
                    raiseAt = CaptureResource.CameraSurface,
                    raise = { raised },
                    videoCloseRaises = { closeFailure },
                    terminationRaises = { listenerFailure },
                )
            val session = rig.wire(make.make(rig))
            val outcome = runCatching { session.start(GrantedFlow(GENERATION), available, available) }
            assertSame(raised, outcome.exceptionOrNull(), "the original leaves; a cleanup failure never replaces it")
            assertTrue(raised.suppressed.any { it === closeFailure }, "carrying the earlier close failure: ${raised.suppressed.toList()}")
            assertTrue(raised.suppressed.any { it === listenerFailure }, "and the later notification failure")
            assertTrue(rig.audio.handoff.ended, "the close after the failing one still happened")
            assertTrue(rig.ownership.isClosed, "and so did the ownership's")
            assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "announced once, and not retried")
        }
    }

    /**
     * The user leaves while the driver is still submitting. There is no
     * readiness to wait for any more, so the start says so by name rather than
     * claiming a startup that is already over.
     */
    private fun t12_aTerminalThatWinsInsideBeginRefusesTheStart(make: Make) {
        val session = java.util.concurrent.atomic.AtomicReference<CaptureSession?>(null)
        val rig = Rig(duringBegin = { session.get()?.stop(null) })
        val live = rig.wire(make.make(rig))
        session.set(live)
        val start = assertIs<CaptureStart.Refused>(live.start(GrantedFlow(GENERATION), available, available), "the startup ended inside the driver")
        assertTrue(start.reason.contains("ended"), "by name: ${start.reason}")
        assertNull(live.video, "and a startup that never completed publishes no tracks")
        assertNull(live.audio)
        assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "the ending, once")
        assertEquals(0, rig.listener.all().count { it is CaptureReady }, "and never a readiness")
        assertTrue(rig.ownership.isClosed)
    }

    /**
     * The ownership REPORTS a cleanup failure while a track close THROWS one.
     * Two channels, one ending: the thrown failure carries the reported one
     * rather than making it unreachable.
     */
    private fun t13_aReportedOwnershipFailureIsNotLostBehindAThrownOne(make: Make) {
        val reported = IOException("the encoder's close reported a failure")
        val thrown = IllegalStateException("the video track's close threw")
        val rig =
            Rig(
                videoCloseRaises = { thrown },
                reportOnClose = mapOf(CaptureResource.VideoEncoder to reported),
            )
        val session = rig.wire(make.make(rig))
        rig.startAndSettle(session)
        val outcome = runCatching { session.stop(null) }
        assertSame(thrown, outcome.exceptionOrNull(), "the thrown failure is what escapes")
        assertTrue(
            thrown.suppressed.any { it === reported },
            "the ownership's returned cleanup error must not disappear: ${thrown.suppressed.toList()}",
        )
        assertTrue(rig.audio.handoff.ended, "the track after the failing one still closed")
        assertTrue(rig.ownership.isClosed, "and so did the ownership")
        assertEquals(
            rig.acquirer.order.size + 1,
            rig.platform.closed().size,
            "every acquired resource was closed exactly once",
        )
        assertEquals(1, rig.listener.all().count { it is CaptureTermination }, "one ending")
    }

    /** With nothing thrown, that same reported failure is RETURNED, exactly as before. */
    private fun t14_aReportedOwnershipFailureWithNothingThrownIsStillReturned(make: Make) {
        val reported = IOException("the encoder's close reported a failure")
        val rig = Rig(reportOnClose = mapOf(CaptureResource.VideoEncoder to reported))
        val session = rig.wire(make.make(rig))
        rig.startAndSettle(session)
        val outcome = runCatching { session.stop(null) }
        assertTrue(outcome.isSuccess, "a reported failure is returned, never newly thrown: ${outcome.exceptionOrNull()}")
        assertSame(reported, outcome.getOrNull(), "and it is the one the ownership reported")
        assertTrue(rig.ownership.isClosed)
        assertEquals(1, rig.listener.all().count { it is CaptureTermination })
    }

    /** An Error primary keeps its identity and carries what the ownership reported. */
    private fun t15_anErrorPrimaryCarriesTheReportedOwnershipFailure(make: Make) {
        val raised = AssertionError("an Error raised inside begin")
        val reported = IOException("the encoder's close reported a failure")
        val rig =
            Rig(
                raiseAt = CaptureResource.CameraSurface,
                raise = { raised },
                reportOnClose = mapOf(CaptureResource.VideoEncoder to reported),
            )
        val session = rig.wire(make.make(rig))
        val outcome = runCatching { session.start(GrantedFlow(GENERATION), available, available) }
        assertSame(raised, outcome.exceptionOrNull(), "the Error leaves by identity")
        assertTrue(
            raised.suppressed.any { it === reported },
            "carrying what the ownership reported, which it could not be handed: ${raised.suppressed.toList()}",
        )
        assertTrue(raised.suppressed.none { it === raised }, "and never itself")
        assertTrue(rig.ownership.isClosed)
        assertEquals(1, rig.listener.all().count { it is CaptureTermination })
    }

    private fun runContract(make: Make) {
        t1_permissionsAndBothCapabilityFactsGateAcquisition(make)
        t2_startIsPendingUntilTheAsynchronousStepsSettleThenReady(make)
        t3_aPartialAcquisitionUnwindsTheExpectedPrefix(make)
        t4_bothTracksFeedTheAcceptedPipelineThroughIdleOutputIdleAndEos(make)
        t5_theFirstFaultArrivesThroughTheProductionRouteAndTerminatesOnce(make)
        t6_everyEndingConvergesOnOneIdempotentStop(make)
        t7_theSameTrackEndingTwiceAndAForeignEndingCompleteNothing(make)
        t8_startIsClaimedOnceAndNeverAfterTheEnding(make)
        t9_everyMandatoryCloseIsAttemptedWhenAnEarlierOneThrows(make)
        t10_aDriverThatThrowsAfterAdoptingUnwindsAndKeepsItsFailure(make)
        t11_aCleanupFailureDuringTheStartupUnwindIsCarriedNotSubstituted(make)
        t12_aTerminalThatWinsInsideBeginRefusesTheStart(make)
        t13_aReportedOwnershipFailureIsNotLostBehindAThrownOne(make)
        t14_aReportedOwnershipFailureWithNothingThrownIsStillReturned(make)
        t15_anErrorPrimaryCarriesTheReportedOwnershipFailure(make)
    }

    private val real = Make { rig -> PlatformCaptureSession(GENERATION, rig.ownership, rig.acquirer, rig.videoTrack, rig.audio.handoff, rig.listener) }
    private val conforming = Make { rig -> ConformingSession(GENERATION, rig.ownership, rig.acquirer, rig.videoTrack, rig.audio.handoff, rig.listener) }

    @Test
    fun t_theCaptureSessionMeetsItsContract() = runContract(real)

    @Test
    fun s_theContractIsSatisfiableByAConformingFixture() = runContract(conforming)

    /**
     * The whole sample assembly, with no host fixture standing in for anything
     * the sample ships: the accepted permission flow settled through real
     * results, the accepted ownership, both accepted handoffs over the accepted
     * ledger, and the session. Only the PLATFORM is scripted -- the handles the
     * driver opens, the dispatcher its callbacks arrive on, and the codec
     * buffers -- which is what a host has instead of a camera.
     */
    @Test
    fun p1_theProductComponentsAssembleIntoOneStartingPublishingAndEndingSession() {
        val permissions = CapturePermissionFlow(GENERATION)
        assertEquals(
            setOf(CapturePermission.Camera, CapturePermission.Microphone),
            permissions.request(CapturePermission.entries.toSet()),
            "both permissions are asked for, through the accepted flow",
        )
        for (permission in CapturePermission.entries) {
            assertEquals(
                PermissionOutcome.Settled(permission, PermissionState.Granted),
                permissions.onResult(GENERATION, permission, PermissionFacts(granted = true, mayExplain = false)),
                "and each is settled by its own result",
            )
        }
        assertEquals(CaptureReadiness.Ready, permissions.readiness())

        val platform = Platform()
        val ownership = OrderedCaptureOwnership(GENERATION, HostDispatcher(), LateLog())
        val acquirer = ScriptedAcquirer(platform)
        val forward = Forward()
        val videoBuffers = HostBuffers()
        val videoReleases = HostReleases()
        val videoLedger = BoundedCaptureLedger(MediaKind.Video, GENERATION, capacity = 16)
        val video = BoundedCaptureHandoff(MediaKind.Video, GENERATION, videoBuffers, videoReleases, videoLedger, forward, capacity = 4, maxOutputBytes = HOST_MAX_OUTPUT_BYTES)
        val audioLedger = BoundedCaptureLedger(MediaKind.Audio, GENERATION, capacity = 16)
        val audio = BoundedCaptureHandoff(MediaKind.Audio, GENERATION, HostBuffers(), HostReleases(), audioLedger, forward, capacity = 4, maxOutputBytes = HOST_MAX_OUTPUT_BYTES)
        val listener = HostSessionListener()
        val session = PlatformCaptureSession(GENERATION, ownership, acquirer, video, audio, listener)
        forward.to = session

        assertEquals(CaptureStart.Pending, session.start(permissions, available, available), "the camera settles by callback")
        assertEquals(emptyList(), listener.all(), "nothing is claimed before the platform calls back")
        ownership.dispatcher.deliver { assertEquals(AdoptOutcome.Adopted, acquirer.settle(ownership, CaptureResource.Camera)) }
        ownership.dispatcher.deliver { assertEquals(AdoptOutcome.Adopted, acquirer.settle(ownership, CaptureResource.CameraStream)) }
        assertEquals(listOf<Any>(CaptureReady.Ready), listener.all(), "ready once, after the last real prerequisite")
        assertSame(video, session.video, "and the tracks are the ones it was given")
        assertSame(audio, session.audio)

        // One real output: observed in the product ledger, copied by the
        // product handoff, judged by the accepted pipeline.
        val bytes = VIDEO_ANNEXB.copyOf()
        videoBuffers.put(0, ByteBuffer.wrap(bytes))
        assertEquals(ObserveOutcome.Recorded, videoLedger.observe(GENERATION, MediaKind.Video, CaptureObservation(33_333L, 1_000_500_000L)))
        assertEquals(HandoffOutcome.Queued, video.onOutput(GENERATION, CodecOutput(0, 0, bytes.size, 33_333L, CaptureContract.FLAG_KEY_FRAME)))
        val item = assertNotNull(video.next())
        val clock = PlatformCaptureClock(ClockCalibration(elapsedRealtimeNanos = 1_000_000_000L, epoch = UnixMicros(EPOCH_BASE), uncertaintyMicros = 10L))
        videoWrite(PublishPipeline(MediaKind.Video, PublishTimeline(0L), clock, validatedVideo(), HOST_LIMITS), item, VIDEO_AVC1)
        assertEquals(listOf(0), videoReleases.indices(), "the codec's index went back exactly once")
        assertNull(video.next(), "and the track is idle again")

        // The real fault route: the track reports through its sink, which is
        // this session, and the ending closes everything once.
        video.onCodecError(GENERATION, "CodecException: the encoder died")
        val ended = assertIs<CaptureTermination.Failed>(session.termination)
        assertEquals(CaptureFault.Codec(MediaKind.Video, "CodecException: the encoder died"), ended.fault)
        assertTrue(video.ended && audio.ended, "both tracks closed")
        assertTrue(ownership.isClosed)
        val installed =
            acquirer.order.filterNot { it == CaptureResource.Camera || it == CaptureResource.CameraStream } +
                listOf(CaptureResource.Camera, CaptureResource.CameraStream)
        assertEquals(
            installed.reversed().map { platform.identity(it) },
            platform.closed(),
            "every acquired resource closed once, in reverse adoption order",
        )
        assertEquals(listOf<Any>(CaptureReady.Ready, ended), listener.all(), "ready, then the ending, once each")
        assertEquals(0, ownership.pending, "nothing is left outstanding")
    }

    // ---- discrimination: the right COUNT of tracks, the wrong ones ------------

    /** One video, one audio, started, terminated once -- but its OWN handoffs, and no gating. */
    private class OwnHandoffSession(override val generation: Long, private val ownership: CaptureOwnership, private val listener: CaptureSessionListener) : CaptureSession {
        override var video: CaptureHandoff? = null
            private set
        override var audio: CaptureHandoff? = null
            private set
        override var termination: CaptureTermination? = null
            private set

        override fun start(permissions: PermissionFlow, cameraFacts: CaptureCapability, audioFacts: CaptureCapability): CaptureStart {
            val faults = HostFaults()
            video = ConformingHandoff(MediaKind.Video, generation, HostBuffers(), HostReleases(), HostLedger(MediaKind.Video, generation), faults, 4)
            audio = ConformingHandoff(MediaKind.Audio, generation, HostBuffers(), HostReleases(), HostLedger(MediaKind.Audio, generation), faults, 4)
            listener.onReady(generation, CaptureReady.Ready)
            return CaptureStart.Pending
        }

        override fun onTrackEnded(generation: Long, kind: MediaKind) = Unit

        /** This generation, and no termination recorded; this stand-in has no lock. */
        override fun admitPublication(generation: Long): Boolean = generation == this.generation && termination == null

        /** This stand-in closes nothing, delivers its ending nowhere and admits no format callbacks, so an ending it recorded leaves no causes or cleanup behind; with none recorded there is nothing to receive. */
        override suspend fun retirement(): RetirementReceipt = RetirementReceipt(checkNotNull(termination) { "no ending was claimed" }, causes = emptyList(), cleanup = emptyList())

        override fun onFault(generation: Long, fault: CaptureFault) {
            if (termination == null) termination = CaptureTermination.Failed(fault)
        }

        override fun onDiscarded(generation: Long, kind: MediaKind, outputs: Int) = Unit

        override fun stop(primary: Exception?): Exception? {
            if (termination == null) termination = CaptureTermination.Completed
            ownership.close(primary)
            return primary
        }
    }

    private val own = Make { rig -> OwnHandoffSession(GENERATION, rig.ownership, rig.listener) }

    @Test
    fun m1_aSessionThatBuildsItsOwnHandoffsFails() {
        assertFailsWithAssertion { t2_startIsPendingUntilTheAsynchronousStepsSettleThenReady(own) }
        assertFailsWithAssertion { t5_theFirstFaultArrivesThroughTheProductionRouteAndTerminatesOnce(own) }
    }

    @Test
    fun m2_aSessionThatStartsWithoutTheGatingFactsFails() = assertFailsWithAssertion { t1_permissionsAndBothCapabilityFactsGateAcquisition(own) }

    private fun assertFailsWithAssertion(body: () -> Unit) {
        val thrown =
            try {
                body()
                null
            } catch (assertion: AssertionError) {
                assertion
            }
        assertTrue(thrown != null, "this row is satisfied by a session with the right NUMBER of tracks")
    }

    /**
     * Same payload, same presentation time, one wrong instant. It has to be
     * rejected where the instant is judged, by that assertion, not by any
     * failure that happens to occur.
     */
    @Test
    fun m3_aWrongCaptureInstantIsRejectedAtTheInstantAssertion() {
        val forward = Forward()
        val track = Track(MediaKind.Video, forward)
        val bytes = VIDEO_ANNEXB.copyOf()
        assertEquals(HandoffOutcome.Queued, track.handoff.onOutput(GENERATION, track.output(0, bytes, pts = 33_333L, captureNanos = 1_000_500_000L)))
        val item = assertNotNull(track.handoff.next())
        val clock = PlatformCaptureClock(ClockCalibration(elapsedRealtimeNanos = 1_000_000_000L, epoch = UnixMicros(EPOCH_BASE), uncertaintyMicros = 10L))
        videoWrite(PublishPipeline(MediaKind.Video, PublishTimeline(0L), clock, validatedVideo(), HOST_LIMITS), item, VIDEO_AVC1)

        val skewed = CapturedOutput(item.output, item.captureElapsedNanos + 1_000_000L)
        val thrown =
            assertFailsWith<AssertionError> {
                videoWrite(PublishPipeline(MediaKind.Video, PublishTimeline(0L), clock, validatedVideo(), HOST_LIMITS), skewed, VIDEO_AVC1)
            }
        assertTrue(
            thrown.message.orEmpty().contains(CAPTURE_INSTANT),
            "the stand-in must be rejected by the instant assertion, not by bytes or presentation time: ${thrown.message}",
        )
    }

    private companion object {
        const val CAPTURE_INSTANT = "the capture instant is the observation the ledger claimed for this output, through the calibrated clock"
        const val EPOCH_BASE = 1_700_000_000_000_000uL

        const val GENERATION = 1L
    }
}
