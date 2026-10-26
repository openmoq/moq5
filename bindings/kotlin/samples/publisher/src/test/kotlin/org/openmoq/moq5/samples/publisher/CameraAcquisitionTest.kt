package org.openmoq.moq5.samples.publisher

import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The Camera2 acquisition, driven through the real session, ownership,
 * handoff and [TrackEncoder], with the camera and the encoder scripted: every
 * platform call and close is logged, in order, and every callback is
 * delivered when the row says -- inline inside the call that can settle it,
 * after a stop, twice, or from a [HostWorker] held inside the ownership's own
 * settlement. `Camera2Capture.kt` implements [CameraPort] over Camera2 and is
 * compiled against the SDK; these rows cannot execute it.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class CameraAcquisitionTest {

    /** A scripted opened camera: its stream submission is logged, may throw, and may settle inline. */
    private class Camera(val name: String, private val log: MutableList<String>) : CameraLink {
        override val identity: Any = HostResource(name)

        @Volatile var surface: Any? = null

        @Volatile var events: StreamEvents? = null

        @Volatile var createFailure: Throwable? = null

        @Volatile var onCreate: (StreamEvents) -> Unit = {}

        override fun close() {
            synchronized(log) { log.add("close:$name") }
        }

        override fun createStream(surface: Any, events: StreamEvents) {
            synchronized(log) { log.add("create-stream:$name") }
            createFailure?.let { throw it }
            this.surface = surface
            this.events = events
            onCreate(events)
        }
    }

    /** A scripted configured stream: its repeating request is logged and may be refused. */
    private class Stream(val name: String, private val log: MutableList<String>) : StreamLink {
        override val identity: Any = HostResource(name)

        @Volatile var target: Any? = null

        @Volatile var fps: IntRange? = null

        @Volatile var repeatFailure: Throwable? = null

        override fun close() {
            synchronized(log) { log.add("close:$name") }
        }

        override fun startRepeating(surface: Any, fps: IntRange) {
            synchronized(log) { log.add("repeat:$name") }
            repeatFailure?.let { throw it }
            target = surface
            this.fps = fps
        }
    }

    /** The scripted camera port: its facts, its open, and the callbacks the open was given. */
    private class Port(private val log: MutableList<String>) : CameraPort {
        @Volatile var facts = CameraFacts(listOf(1280 to 720, 640 to 480), listOf(15..30, 24..24, 30..30))

        @Volatile var factsFailure: Throwable? = null

        @Volatile var openFailure: Throwable? = null

        @Volatile var onOpen: (CameraEvents) -> Unit = {}

        @Volatile var events: CameraEvents? = null

        override fun facts(): CameraFacts {
            synchronized(log) { log.add("facts") }
            factsFailure?.let { throw it }
            return facts
        }

        override fun open(events: CameraEvents) {
            synchronized(log) { log.add("open-camera") }
            openFailure?.let { throw it }
            this.events = events
            onOpen(events)
        }
    }

    /** Holds the Nth settlement the ownership makes, before it decides, until the row lets it go. */
    private class HoldSettlement(private val nth: Int) : AdoptionSchedule {
        private val count = AtomicInteger()
        val reached = HostGate("the held settlement reached the ownership")
        val release = HostGate("the row lets the held settlement go")

        override fun beforeSettle() {
            if (count.incrementAndGet() == nth) {
                reached.open()
                release.await()
            }
        }
    }

    /** The acquirer's late-cleanup sink: every report, in order, and an optional raise of its own. */
    private class RaisingLate(private val raise: Throwable?) : LateCleanupSink {
        private val reports = mutableListOf<Pair<CaptureResource, Exception?>>()

        @Synchronized
        override fun onLateClose(generation: Long, resource: CaptureResource, failure: Exception?) {
            reports.add(resource to failure)
            raise?.let { throw it }
        }

        @Synchronized
        override fun onForeignDisposal(generation: Long, resource: CaptureResource, failure: Exception?) {
            reports.add(resource to failure)
        }

        @Synchronized
        fun all(): List<Pair<CaptureResource, Exception?>> = reports.toList()
    }

    /**
     * The real session over the camera acquirer: the video track's handoff
     * leases the real [TrackEncoder]; the audio track is an idle lane, since
     * this acquirer acquires nothing for audio.
     */
    private class Rig(
        schedule: AdoptionSchedule = AdoptionSchedule.NONE,
        workerFailure: Throwable? = null,
        /** The acquirer's own late sink, when it is not the ownership's. */
        acquirerLate: LateCleanupSink? = null,
    ) {
        val log = mutableListOf<String>()
        val backend = HostEncoderBackend(log)
        val video = TrackEncoder(VIDEO, backend)
        val port = Port(log)
        val camera = Camera("camera", log)
        val stream = Stream("stream", log)
        val dispatcher = HostDispatcher()
        private val worker =
            WorkerStart {
                synchronized(log) { log.add("open:worker") }
                workerFailure?.let { throw it }
                object : ResourceHandle {
                    override val identity: Any = HostResource("worker")

                    override fun close() {
                        synchronized(log) { log.add("quit:worker") }
                        dispatcher.quit()
                    }
                }
            }
        val late = LateLog()
        val acquirer = SequencedAcquirer(worker, CameraPart(video, port, acquirerLate ?: late), null)
        val ownership = OrderedCaptureOwnership(GENERATION, dispatcher, late, schedule)
        val forward = Forward()
        val videoLedger = BoundedCaptureLedger(MediaKind.Video, GENERATION, capacity = 8)
        val videoHandoff = BoundedCaptureHandoff(MediaKind.Video, GENERATION, video, video, videoLedger, forward, CAPACITY, HOST_MAX_OUTPUT_BYTES, lifetime = video)
        val audio = Lane(MediaKind.Audio, forward)
        val listener = HostSessionListener()
        val session = PlatformCaptureSession(GENERATION, ownership, acquirer, videoHandoff, audio.handoff, listener)

        init {
            forward.to = session
            acquirer.bindFaults(forward)
            video.bind(EncoderRoute(session, session.bindCodecCallbacks(MediaKind.Video), GENERATION, videoHandoff) { _, _ -> })
        }

        fun start(): CaptureStart = session.start(Granted(GENERATION), CaptureCapability.Available, CaptureCapability.Available)

        fun logNow(): List<String> = synchronized(log) { log.toList() }

        fun opened() = checkNotNull(port.events).onOpened(camera)

        fun configured() = checkNotNull(camera.events).onConfigured(stream)

        /** The platform's `onClosed` for [which]: the device has fully shut down. */
        fun closed(which: CameraLink = camera) = checkNotNull(port.events).onClosed(which)

        fun count(entry: String): Int = logNow().count { it == entry }

        /** The entries of [wanted], in the order they were logged. */
        fun ordered(vararg wanted: String): List<String> = logNow().filter { it in wanted }
    }

    private val opening =
        listOf("facts", "open:worker", "create:Video", "callbacks:video", "configure:video", "surface:video", "start:video", "open-camera")

    private fun encoderDisposal() = arrayOf("stop:video", "release-codec:video", "surface-release:video")

    /** The ending's settlement: the worker quit once, last, and every protection balanced. */
    private suspend fun settled(rig: Rig, label: String): RetirementReceipt {
        assertEquals(1, rig.count("quit:worker"), "$label: ${rig.logNow()}")
        assertEquals("quit:worker", rig.logNow().last(), "$label: ${rig.logNow()}")
        assertFalse(rig.ownership.retainWorker(), label)
        assertReservationsBalanced(rig.ownership, label)
        return rig.session.retirement()
    }

    // -- success --------------------------------------------------------------------------------

    @Test
    fun `c1 worker, encoder, camera and stream in order, the stream on the encoder's own surface, and a teardown that stops the camera before the encoder`() = runTest {
        val rig = Rig()
        assertEquals(CaptureStart.Pending, rig.start(), "the camera is still opening")
        assertEquals(opening, rig.logNow(), "the camera is opened only once the encoder, and its surface, exist")
        rig.opened()
        assertEquals("create-stream:camera", rig.logNow().last(), "the stream is submitted from onOpened")
        assertEquals(emptyList(), rig.listener.all(), "no readiness before the stream runs")
        rig.configured()
        assertEquals("repeat:stream", rig.logNow().last())
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.listener.all())
        val surface = rig.video.inputSurface
        assertSame(surface, rig.camera.surface, "the stream's one output is the encoder's input surface")
        assertSame(surface, rig.stream.target, "and so is the repeating request's target")
        assertEquals(30..30, rig.stream.fps, "the fixed range at the encoder's rate")
        assertIs<LostOutcome.Ignored>(rig.ownership.lost(CaptureResource.CameraSurface, "probe"), "the surface is the encoder's, never installed a second time")

        rig.session.stop(null)
        assertEquals(listOf("close:stream", "close:camera"), rig.ordered("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), "the stream and then the camera are closed")
        assertTrue(rig.dispatcher.isAlive, "and the worker the camera's onClosed arrives on is kept")
        rig.closed()
        assertEquals(
            listOf("close:stream", "close:camera", *encoderDisposal(), "quit:worker"),
            rig.ordered("close:stream", "close:camera", *encoderDisposal(), "quit:worker"),
            "the encoder that owns the surface is disposed only once the camera has fully shut down",
        )
        assertEquals(1, rig.count("surface-release:video"), "released once, by its one owner")
        assertEquals(CaptureTermination.Completed, settled(rig, "success").termination)
    }

    @Test
    fun `c2 callbacks delivered inline inside the call that can settle them are counted, and the start is ready`() = runTest {
        val rig = Rig()
        rig.port.onOpen = { it.onOpened(rig.camera) }
        rig.camera.onCreate = { it.onConfigured(rig.stream) }
        assertEquals(CaptureStart.Started, rig.start(), "every step settled inside begin, behind the startup guard")
        assertEquals(opening + listOf("create-stream:camera", "repeat:stream"), rig.logNow())
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.listener.all(), "ready once, after the stream runs")
        rig.session.stop(null)
        rig.closed()
        settled(rig, "inline")
    }

    // -- late and duplicate callbacks -------------------------------------------------------------

    @Test
    fun `c3 a camera that opens after the stop is closed, gets no stream, and keeps the worker until it settles`() = runTest {
        val rig = Rig()
        assertEquals(CaptureStart.Pending, rig.start())
        rig.session.stop(null)
        assertEquals(listOf(*encoderDisposal()), rig.ordered(*encoderDisposal()))
        assertEquals(0, rig.count("quit:worker"), "the camera's ticket is still outstanding, so its worker is kept")
        assertTrue(rig.dispatcher.isAlive)
        rig.opened()
        assertEquals(1, rig.count("close:camera"), "the late camera is closed once")
        assertEquals(0, rig.count("create-stream:camera"), "and never given the surface")
        assertTrue("late-close:Camera" in rig.late.all(), "${rig.late.all()}")
        settled(rig, "late open")
    }

    @Test
    fun `c4 a duplicate callback settles nothing again, and a foreign camera is disposed of once`() = runTest {
        val rig = Rig()
        rig.start()
        rig.opened()
        rig.configured()
        rig.opened()
        rig.configured()
        assertEquals(1, rig.count("create-stream:camera"), "no second stream")
        assertEquals(1, rig.count("repeat:stream"), "no second request")
        assertEquals(0, rig.count("close:camera") + rig.count("close:stream"), "a duplicate of what is installed closes nothing")
        assertEquals(listOf<Any>(CaptureReady.Ready), rig.listener.all(), "ready once")

        checkNotNull(rig.port.events).onOpened(Camera("other", rig.log))
        assertEquals(1, rig.count("close:other"), "a camera nobody asked for is closed once")
        assertEquals(listOf("foreign:Camera"), rig.late.all(), "and reported as a foreign disposal")

        rig.session.stop(null)
        rig.closed(Camera("other", rig.log))
        assertEquals(0, rig.count("stop:video"), "a foreign camera's onClosed releases nothing")
        rig.closed()
        rig.closed()
        assertEquals(1, rig.count("release-codec:video"), "a duplicate onClosed releases nothing twice")
        assertEquals(1, rig.count("close:camera"))
        assertEquals(1, rig.count("close:stream"))
        settled(rig, "duplicate")
    }

    // -- failures -----------------------------------------------------------------------------------

    @Test
    fun `c5 an open that fails carrying the device closes it once, submits no stream, and refuses the startup`() = runTest {
        for (how in listOf("error", "disconnected")) {
            val rig = Rig()
            assertEquals(CaptureStart.Pending, rig.start())
            val events = checkNotNull(rig.port.events)
            if (how == "error") events.onError(rig.camera, 4) else events.onDisconnected(rig.camera)
            val reason = if (how == "error") "the camera reported error 4" else "the camera was disconnected"
            assertEquals(1, rig.count("close:camera"), how)
            assertEquals(0, rig.count("create-stream:camera"), how)
            assertEquals(listOf<Any>(CaptureReady.Refused("Camera: $reason"), CaptureTermination.Cancelled("Camera: $reason")), rig.listener.all(), how)
            assertEquals(listOf("close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:camera", *encoderDisposal(), "quit:worker"), how)
            settled(rig, how)
        }
    }

    @Test
    fun `c6 a stream that fails to configure is not closed again, and the camera and encoder unwind`() = runTest {
        val rig = Rig()
        rig.start()
        rig.opened()
        checkNotNull(rig.camera.events).onConfigureFailed(rig.stream)
        assertEquals(0, rig.count("close:stream"), "a failed session is already closed")
        assertEquals(0, rig.count("repeat:stream"))
        assertEquals(CaptureTermination.Cancelled("CameraStream: the capture stream could not be configured"), rig.session.termination)
        assertEquals(0, rig.count("stop:video"), "the camera was given the surface, so the encoder waits for its onClosed")
        rig.closed()
        assertEquals(listOf("close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:camera", *encoderDisposal(), "quit:worker"))
        settled(rig, "configure failed")
    }

    @Test
    fun `c7 a camera lost after it was installed is device loss, not a second result, and fails the video track`() = runTest {
        val rig = Rig()
        rig.start()
        rig.opened()
        rig.configured()
        checkNotNull(rig.port.events).onError(rig.camera, 3)
        assertEquals(CaptureTermination.Failed(CaptureFault.Source(MediaKind.Video, "the camera reported error 3")), rig.session.termination)
        assertEquals(1, rig.count("close:camera"), "the lost camera is closed once, and skipped by the teardown")
        assertEquals(1, rig.count("close:stream"))
        assertEquals(listOf<Any>(CaptureReady.Ready, CaptureTermination.Failed(CaptureFault.Source(MediaKind.Video, "the camera reported error 3"))), rig.listener.all(), "one readiness, one ending")
        assertEquals(0, rig.count("release-codec:video"), "the encoder waits for the lost camera's onClosed")
        rig.closed()
        assertEquals(listOf("close:camera", "release-codec:video"), rig.ordered("close:camera", "release-codec:video"), "the camera closed before the encoder")
        settled(rig, "lost")
    }

    @Test
    fun `c8 what the camera reports refuses the start before anything is started, opened or submitted`() {
        class Case(val label: String, val script: (Port) -> Unit, val reason: String)
        val cases =
            listOf(
                Case("size", { it.facts = CameraFacts(listOf(1280 to 720), listOf(30..30)) }, "the camera cannot stream 640x480 into an encoder surface"),
                Case("rate", { it.facts = CameraFacts(listOf(640 to 480), listOf(15..24, 24..24)) }, "the camera has no target frame-rate range ending at 30 fps"),
                Case("facts", { it.factsFailure = IllegalStateException("characteristics unavailable") }, "the camera's characteristics could not be read: characteristics unavailable"),
            )
        for (case in cases) {
            val rig = Rig()
            case.script(rig.port)
            assertEquals(CaptureStart.Refused(case.reason), rig.start(), case.label)
            assertEquals(listOf("facts"), rig.logNow(), "${case.label}: nothing started, opened or submitted")
        }
        // The range choice itself: exactly the encoder's rate when offered, else the highest floor ending there.
        assertEquals(CameraPlan.Planned(15..30), CameraCheck.plan(VIDEO, CameraFacts(listOf(640 to 480), listOf(7..30, 15..30, 30..60))))
    }

    @Test
    fun `c9 a partial setup unwinds exactly what was acquired`() = runTest {
        // The worker cannot start: nothing else is attempted.
        val noWorker = Rig(workerFailure = IllegalStateException("no thread"))
        assertEquals(CaptureStart.Refused("no thread"), noWorker.start())
        assertEquals(listOf("facts", "open:worker"), noWorker.logNow())

        // The encoder cannot be created: the worker is released, and no camera opened.
        val noEncoder = Rig()
        noEncoder.backend.refuse = "no AVC encoder"
        assertEquals(CaptureStart.Refused("the Video encoder cannot be configured: no AVC encoder"), noEncoder.start())
        assertEquals(listOf("facts", "open:worker", "create:Video", "quit:worker"), noEncoder.logNow())

        // openCamera throws where it stands: its ticket settles once, and the encoder and worker unwind.
        val noOpen = Rig()
        noOpen.port.openFailure = SecurityException("the CAMERA permission is not granted")
        assertEquals(CaptureStart.Refused("the camera could not be opened: the CAMERA permission is not granted"), noOpen.start())
        assertEquals(opening + listOf(*encoderDisposal(), "quit:worker"), noOpen.logNow())
        settled(noOpen, "open refused")

        // The stream cannot be submitted inside onOpened: the camera it would have run on unwinds too.
        val noStream = Rig()
        noStream.camera.createFailure = IllegalStateException("the device is closed")
        noStream.start()
        noStream.opened()
        assertEquals(CaptureTermination.Cancelled("CameraStream: the capture stream could not be submitted: the device is closed"), noStream.session.termination)
        noStream.closed()
        assertEquals(listOf("close:camera", *encoderDisposal(), "quit:worker"), noStream.ordered("close:camera", *encoderDisposal(), "quit:worker"))
        assertEquals(0, noStream.count("repeat:stream"))
        settled(noStream, "stream refused")
    }

    @Test
    fun `c10 a repeating request that cannot start closes its stream once and the startup is refused, never ready`() = runTest {
        val rig = Rig()
        rig.stream.repeatFailure = IllegalStateException("the request was refused")
        rig.start()
        rig.opened()
        rig.configured()
        val reason = "CameraStream: the repeating request could not start: the request was refused"
        assertEquals(listOf<Any>(CaptureReady.Refused(reason), CaptureTermination.Cancelled(reason)), rig.listener.all())
        rig.closed()
        assertEquals(listOf("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:stream", "close:camera", *encoderDisposal(), "quit:worker"))
        assertEquals(1, rig.count("close:stream"))
        settled(rig, "repeat refused")
    }

    // -- a stop against a held callback ------------------------------------------------------------

    @Test
    fun `c11 a stop while the stream's settlement is held closes the camera before the encoder, and the late stream is closed without a request`() = runTest {
        // The camera's adoption is the first settlement; the stream's is the second.
        val hold = HoldSettlement(nth = 2)
        val rig = Rig(hold)
        rig.start()
        rig.opened()
        val callback = HostWorker("camera-callback") { rig.configured() }.start()
        var workerAlive = false
        withSettlement(listOf(hold.release), listOf(callback)) {
            hold.reached.await(callback)
            rig.session.stop(null)
            workerAlive = rig.dispatcher.isAlive
            hold.release.open()
            callback.join()
        }
        assertTrue(workerAlive, "the stream's ticket kept the worker while its settlement was held")
        assertEquals(0, rig.count("repeat:stream"), "a stream that arrives after the stop gets no request")
        rig.closed()
        assertEquals(
            listOf("close:camera", "close:stream", *encoderDisposal(), "quit:worker"),
            rig.ordered("close:camera", *encoderDisposal(), "close:stream", "quit:worker"),
            "the camera stops, the late stream is closed once, and only the camera's onClosed lets the encoder go; then the worker quits",
        )
        assertNull(rig.listener.all().firstOrNull { it == CaptureReady.Ready })
        settled(rig, "held stream")
    }

    // -- a stream still configuring when the camera goes -------------------------------------------

    @Test
    fun `d1 a camera lost while its stream is still configuring settles that stream once, and a late stream callback settles nothing`() = runTest {
        for (late in listOf("configured", "configure failed")) {
            val rig = Rig()
            rig.start()
            rig.opened()
            checkNotNull(rig.port.events).onError(rig.camera, 5)
            assertEquals(CaptureTermination.Failed(CaptureFault.Source(MediaKind.Video, "the camera reported error 5")), rig.session.termination, "$late: the loss is the ending's cause")
            // A late configure callback can still arrive on the worker, before the device's onClosed.
            val events = checkNotNull(rig.camera.events)
            if (late == "configured") events.onConfigured(rig.stream) else events.onConfigureFailed(rig.stream)
            assertEquals(0, rig.count("repeat:stream"), late)
            assertEquals(if (late == "configured") 1 else 0, rig.count("close:stream"), "$late: a late stream is disposed of once, as nobody's")
            assertEquals(if (late == "configured") listOf("foreign:CameraStream") else emptyList(), rig.late.all().filter { "CameraStream" in it }, late)
            rig.closed()
            assertEquals("quit:worker", rig.logNow().last(), "$late: the pending stream no longer keeps the worker: ${rig.logNow()}")
            assertEquals(1, rig.count("close:camera"), late)
            settled(rig, "lost while configuring, then $late")
        }
    }

    @Test
    fun `d2 a stop while the stream is still configuring settles that stream once, and a late stream callback settles nothing`() = runTest {
        for (late in listOf("configured", "configure failed")) {
            val rig = Rig()
            rig.start()
            rig.opened()
            rig.session.stop(null)
            assertEquals(CaptureTermination.Completed, rig.session.termination, late)
            assertEquals(1, rig.count("close:camera"), late)
            val events = checkNotNull(rig.camera.events)
            if (late == "configured") events.onConfigured(rig.stream) else events.onConfigureFailed(rig.stream)
            assertEquals(0, rig.count("repeat:stream"), late)
            assertEquals(if (late == "configured") 1 else 0, rig.count("close:stream"), late)
            assertEquals(if (late == "configured") listOf("foreign:CameraStream") else emptyList(), rig.late.all().filter { "CameraStream" in it }, late)
            rig.closed()
            assertEquals(listOf("close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:camera", *encoderDisposal(), "quit:worker"), late)
            settled(rig, "stopped while configuring, then $late")
        }
    }

    // -- a platform call that raises a cancellation or an Error --------------------------------------

    private fun raisers(): List<Throwable> = listOf(CancellationException("the call was cancelled"), AssertionError("the call raised an Error"))

    @Test
    fun `d3 an open that raises leaves the startup as itself, never a named refusal, with its ticket settled once`() = runTest {
        for (raised in raisers()) {
            val label = raised::class.simpleName
            val rig = Rig()
            rig.port.openFailure = raised
            assertSame(raised, runCatching { rig.start() }.exceptionOrNull(), "$label: the start leaves as the raise itself")
            assertIs<CaptureTermination.Cancelled>(rig.session.termination, label)
            assertTrue(rig.listener.all().none { it is CaptureReady.Refused }, "$label: no named refusal was announced: ${rig.listener.all()}")
            assertEquals(opening + listOf(*encoderDisposal(), "quit:worker"), rig.logNow(), label)
            settled(rig, "open raised $label")
        }
        // The same raise after the open already settled the camera inline: nothing is settled twice.
        for (raised in raisers()) {
            val label = raised::class.simpleName
            val rig = Rig()
            rig.port.onOpen = { events ->
                events.onOpened(rig.camera)
                throw raised
            }
            assertSame(raised, runCatching { rig.start() }.exceptionOrNull(), "$label inline: the start leaves as the raise itself")
            assertEquals(1, rig.count("close:camera"), "$label inline")
            rig.closed()
            assertEquals(listOf("close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:camera", *encoderDisposal(), "quit:worker"), "$label inline")
            settled(rig, "open raised $label after settling inline")
        }
    }

    @Test
    fun `d4 a stream submission that raises escapes the callback as itself, and the stream and camera settle once`() = runTest {
        for (raised in raisers()) {
            val label = raised::class.simpleName
            val rig = Rig()
            rig.camera.createFailure = raised
            assertEquals(CaptureStart.Pending, rig.start())
            assertSame(raised, runCatching { rig.opened() }.exceptionOrNull(), "$label: the callback's caller receives the raise itself")
            assertEquals(
                CaptureTermination.Failed(CaptureFault.Source(MediaKind.Video, "the capture stream's submission raised $label: ${raised.message}")),
                rig.session.termination,
                label,
            )
            assertTrue(rig.listener.all().none { it is CaptureReady }, "$label: no readiness and no named refusal: ${rig.listener.all()}")
            rig.closed()
            assertEquals(listOf("close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:camera", *encoderDisposal(), "quit:worker"), label)
            settled(rig, "submission raised $label")
        }
    }

    @Test
    fun `d5 a repeating request that raises escapes the callback as itself, and its stream is closed once`() = runTest {
        for (raised in raisers()) {
            val label = raised::class.simpleName
            val rig = Rig()
            rig.stream.repeatFailure = raised
            rig.start()
            rig.opened()
            assertSame(raised, runCatching { rig.configured() }.exceptionOrNull(), "$label: the callback's caller receives the raise itself")
            assertEquals(
                CaptureTermination.Failed(CaptureFault.Source(MediaKind.Video, "the repeating request raised $label: ${raised.message}")),
                rig.session.termination,
                label,
            )
            assertTrue(rig.listener.all().none { it is CaptureReady }, "$label: ${rig.listener.all()}")
            assertEquals(1, rig.count("close:stream"), label)
            rig.closed()
            assertEquals(listOf("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), label)
            settled(rig, "repeat raised $label")
        }
    }

    // -- the encoder's disposal at the camera's onClosed ----------------------------------------------

    @Test
    fun `e1 a disposal failure at onClosed reaches the late sink, and a sink that raises carries it`() = runTest {
        for (raise in listOf<Throwable>(IllegalStateException("the late sink failed"), CancellationException("the late sink was cancelled"), AssertionError("the late sink raised an Error"))) {
            val label = raise::class.simpleName
            val sink = RaisingLate(raise)
            val rig = Rig(acquirerLate = sink)
            val disposal = IllegalStateException("the codec's release failed")
            rig.backend.prepare = { device -> if (device.name == "video") device.releaseFailure = disposal }
            rig.start()
            rig.opened()
            rig.configured()
            rig.session.stop(null)
            val escaped = runCatching { rig.closed() }.exceptionOrNull()
            assertSame(raise, escaped, "$label: the sink's own raise leaves, its class kept")
            assertEquals(1, raise.suppressed.count { it === disposal }, "$label: carrying the disposal failure once, by identity")
            assertEquals(listOf<Pair<CaptureResource, Exception?>>(CaptureResource.VideoEncoder to disposal), sink.all(), "$label: the sink saw the exact failure once")
            rig.closed()
            assertEquals(1, rig.count("release-codec:video"), "$label: a second onClosed disposes of nothing again")
            assertEquals(listOf("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), label)
            settled(rig, "late sink raised $label")
        }
        // A sink that accepts the report: the failure is its, and nothing leaves.
        val sink = RaisingLate(null)
        val rig = Rig(acquirerLate = sink)
        val disposal = IllegalStateException("the codec's release failed")
        rig.backend.prepare = { device -> if (device.name == "video") device.releaseFailure = disposal }
        rig.start()
        rig.opened()
        rig.configured()
        rig.session.stop(null)
        rig.closed()
        assertEquals(listOf<Pair<CaptureResource, Exception?>>(CaptureResource.VideoEncoder to disposal), sink.all())
        settled(rig, "late sink accepted")
    }

    @Test
    fun `d6 a repeating request that raises inside an inline configure, inside onOpened, leaves once as itself`() = runTest {
        for (raised in raisers()) {
            val label = raised::class.simpleName
            val rig = Rig()
            rig.camera.onCreate = { it.onConfigured(rig.stream) }
            rig.stream.repeatFailure = raised
            rig.start()
            assertSame(raised, runCatching { rig.opened() }.exceptionOrNull(), "$label: through both settlements, as itself")
            val ending = CaptureTermination.Failed(CaptureFault.Source(MediaKind.Video, "the repeating request raised $label: ${raised.message}"))
            assertEquals(ending, rig.session.termination, label)
            assertEquals(listOf<Any>(ending), rig.listener.all(), "$label: one terminal notification, no readiness and no refusal")
            assertEquals(1, rig.count("create-stream:camera"), label)
            assertEquals(1, rig.count("repeat:stream"), label)
            assertEquals(1, rig.count("close:stream"), label)
            rig.closed()
            assertEquals(listOf("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), rig.ordered("close:stream", "close:camera", *encoderDisposal(), "quit:worker"), label)
            settled(rig, "inline repeat raised $label")
        }
    }

    // -- composition ------------------------------------------------------------------------------

    @Test
    fun `c12 the deferred composition binds the camera acquirer's fault route and its encoder before anything starts`() {
        val log = mutableListOf<String>()
        val video = TrackEncoder(VIDEO, HostEncoderBackend(log))
        val part = CameraPart(video, Port(log), LateLog())
        val acquirer = SequencedAcquirer(WorkerStart { error("nothing starts during assembly") }, part, null)
        val environment =
            CaptureEnvironment(
                acquirer = acquirer,
                dispatcher = HostDispatcher(),
                late = LateLog(),
                video = video.trackCodec,
                audio = TrackCodec(CodecLifetime.RETAINED, HostBuffers(), HostReleases()),
            )
        composeDeferredBroadcast(
            generation = GENERATION,
            permissions = Granted(GENERATION),
            cameraFacts = CaptureCapability.Available,
            audioFacts = CaptureCapability.Available,
            calibration = ClockCalibration(elapsedRealtimeNanos = BASE_NANOS, epoch = org.openmoq.moq5.UnixMicros(EPOCH), uncertaintyMicros = 10L),
            origin = ORIGIN,
            audioKeying = UnitAudioKeying(48_000, 1024, ORIGIN),
            capacity = CAPACITY,
            maxOutputBytes = HOST_MAX_OUTPUT_BYTES,
            maxNalUnits = HOST_MAX_NAL_UNITS,
            videoName = VIDEO_CONFIG.name,
            audioName = AUDIO_CONFIG.name,
            formatBounds = FIXTURE_BOUNDS,
            formatBudget = kotlin.time.Duration.parse("PT5S"),
            environment = environment,
            tracks = ScriptedTracks(),
            completion = Completion(),
            record = CaptureDiscardRecord(GENERATION),
            writeTimeout = WRITE_TIMEOUT,
            drainTimeout = DRAIN_TIMEOUT,
            videoUtility = ScriptedVideoUtility,
            audioUtility = ScriptedAudioUtility,
        )
        assertEquals(emptyList(), log, "assembly started, created and opened nothing")
        assertEquals(
            "this camera part's faults are already bound",
            kotlin.test.assertFailsWith<IllegalStateException> { part.bindFaults(Forward()) }.message,
            "assembly bound the acquirer to the session's fault route",
        )
        assertEquals(
            "this Video encoder is already bound to a session",
            kotlin.test.assertFailsWith<IllegalStateException> {
                video.bind(EncoderRoute(Rig().session, FormatHolderKey(MediaKind.Video, CallbackRole.Codec), GENERATION, Lane(MediaKind.Video, Forward()).handoff) { _, _ -> })
            }.message,
            "and its encoder to the same session",
        )
    }

    private companion object {
        val VIDEO: VideoEncoderConfiguration = (VideoEncoderConfiguration.of(640, 480, 30, 1_000_000, 1_500_000uL, 2) as EncoderPlan.Planned<VideoEncoderConfiguration>).configuration
    }
}
