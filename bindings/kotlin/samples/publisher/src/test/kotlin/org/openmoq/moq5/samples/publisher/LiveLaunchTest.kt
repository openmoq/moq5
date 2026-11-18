package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.SendBackpressure
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.TrustPolicy
import org.openmoq.moq5.UnixMicros

/**
 * The live camera-and-microphone path's launcher and broadcast, host side.
 *
 * The launcher rows drive the real [SessionController] and
 * [CapturePermissionFlow] with a scripted permission port standing in for the
 * Activity's `requestPermissions`/`onRequestPermissionsResult`; UI posts run
 * inline, as they would already be on the UI thread. The broadcast rows drive
 * [CameraBroadcast] over scripted platform, endpoint and composer ports, so
 * what is pinned is what the live session hands the accepted deferred
 * composition and in what order it owns and closes the endpoint.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class LiveLaunchTest {

    /** The Activity's permission surface: every ask recorded; grants and rationale scripted. */
    private class Port : PermissionPort {
        val asks = mutableListOf<Pair<Int, Set<CapturePermission>>>()
        val explain = mutableMapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true)
        val already = mutableSetOf<CapturePermission>()

        override fun ask(requestCode: Int, permissions: Set<CapturePermission>) {
            asks.add(requestCode to permissions)
        }

        override fun mayExplain(permission: CapturePermission): Boolean = explain.getValue(permission)

        override fun granted(permission: CapturePermission): Boolean = permission in already
    }

    private class Launch(
        scope: TestScope,
        codes: RequestCodes = RequestCodes.fresh(),
        live: ((LiveStart) -> suspend (SessionReporter) -> Unit)? = null,
    ) {
        val rendered = mutableListOf<SessionUpdate>()
        val refusals = mutableListOf<String>()
        val starts = mutableListOf<LiveStart>()
        val cancelled = mutableListOf<Long>()
        val port = Port()
        val controller = SessionController(scope, UiDispatch { it() }) { rendered.add(it) }
        /** Records each start and runs until stopped. */
        private val held: (LiveStart) -> suspend (SessionReporter) -> Unit = { start ->
            { _ ->
                starts.add(start)
                try {
                    awaitCancellation()
                } catch (stopped: CancellationException) {
                    cancelled.add(start.generation)
                    throw stopped
                }
            }
        }
        val launcher = LiveLauncher(controller, port, codes, { refusals.add(it) }, live ?: held)

        fun lastCode(): Int = port.asks.last().first
    }

    private val both = setOf(CapturePermission.Camera, CapturePermission.Microphone)

    // -- permissions --------------------------------------------------------------------------------

    @Test
    fun `l1 both permissions granted start exactly one live session, of that generation`() = runTest {
        val launch = Launch(this)
        launch.launcher.startLive()
        assertEquals(listOf(both), launch.port.asks.map { it.second }, "both asked, once")
        assertEquals(emptyList(), launch.starts, "nothing starts before the answer")
        launch.launcher.onPermissionsResult(launch.lastCode(), mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        val start = launch.starts.single()
        assertEquals(CaptureReadiness.Ready, start.permissions.readiness())
        assertEquals(start.generation, start.permissions.generation, "the flow is that generation's")
        assertTrue(SessionUpdate.Started in launch.rendered)
        launch.controller.toggle { }
        runCurrent()
        assertEquals(listOf(start.generation), launch.cancelled, "Stop cancels the live session")
        assertEquals(SessionUpdate.Stopped, launch.rendered.last())

        // Already granted: nothing is asked, and the session starts.
        val granted = Launch(this)
        granted.port.already.addAll(both)
        granted.launcher.startLive()
        runCurrent()
        assertEquals(emptyList(), granted.port.asks)
        assertEquals(1, granted.starts.size)
        granted.launcher.destroy()
        granted.controller.destroy()
        runCurrent()
    }

    @Test
    fun `l2 a denial is a named refusal that starts nothing, and a new start asks only what was denied`() = runTest {
        val launch = Launch(this)
        launch.launcher.startLive()
        launch.launcher.onPermissionsResult(launch.lastCode(), mapOf(CapturePermission.Camera to false, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(listOf("the camera permission was denied"), launch.refusals)
        assertEquals(emptyList(), launch.starts)
        launch.port.already.add(CapturePermission.Microphone)
        launch.launcher.startLive()
        assertEquals(setOf(CapturePermission.Camera), launch.port.asks.last().second, "only the denied permission is asked again")
        launch.launcher.onPermissionsResult(launch.lastCode(), mapOf(CapturePermission.Camera to true))
        runCurrent()
        assertEquals(1, launch.starts.size)
        launch.launcher.destroy()
        launch.controller.destroy()
        runCurrent()
    }

    @Test
    fun `l3 a denial the platform will not explain again is named as permanent, from the rationale read after the result`() = runTest {
        // The platform would still explain when asked, and stops only with this denial.
        val launch = Launch(this)
        launch.launcher.startLive()
        launch.port.explain[CapturePermission.Microphone] = false
        launch.launcher.onPermissionsResult(launch.lastCode(), mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to false))
        runCurrent()
        assertEquals(listOf("the microphone permission was denied permanently; allow it in Settings"), launch.refusals)
        assertEquals(emptyList(), launch.starts)

        // The same denial while the platform would still explain is an ordinary one.
        val ordinary = Launch(this)
        ordinary.launcher.startLive()
        ordinary.launcher.onPermissionsResult(ordinary.lastCode(), mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to false))
        runCurrent()
        assertEquals(listOf("the microphone permission was denied"), ordinary.refusals)
        assertEquals(emptyList(), ordinary.starts)
    }

    @Test
    fun `l4 a result for a retired request starts nothing, and does not settle the current one`() = runTest {
        val launch = Launch(this)
        launch.launcher.startLive()
        val first = launch.lastCode()
        launch.launcher.onPermissionsResult(first, mapOf(CapturePermission.Camera to false, CapturePermission.Microphone to false))
        launch.launcher.startLive()
        val second = launch.lastCode()
        assertTrue(first != second, "each request has its own code")
        // The first request's result arrives again, late: it is the retired generation's.
        launch.launcher.onPermissionsResult(first, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(emptyList(), launch.starts, "a stale grant starts nothing")
        launch.launcher.onPermissionsResult(second, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(1, launch.starts.size, "the current request still settles")
        launch.launcher.destroy()
        launch.controller.destroy()
        runCurrent()
    }

    @Test
    fun `l5 a recreated instance with its own outstanding request is not settled by its predecessor's result`() = runTest {
        val codes = RequestCodes.fresh()
        val before = Launch(this, codes)
        before.launcher.startLive()
        val old = before.lastCode()
        // Recreation: the predecessor saves its state and is destroyed.
        val saved = codes.saved()
        before.launcher.destroy()
        before.controller.destroy()
        val after = Launch(this, RequestCodes.restored(saved))
        after.launcher.startLive()
        val current = after.lastCode()
        assertTrue(old != current, "the recreated instance's request has its own code")
        // The predecessor's result arrives while the recreated instance's own request is outstanding.
        after.launcher.onPermissionsResult(old, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(emptyList(), after.starts, "the predecessor's grant settles nothing")
        assertEquals(emptyList(), after.refusals)
        assertEquals(emptyList(), before.starts)
        // Only the recreated instance's own result starts its generation.
        after.launcher.onPermissionsResult(current, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(1, after.starts.size, "its own result starts it")
        after.launcher.destroy()
        after.controller.destroy()
        runCurrent()
    }

    @Test
    fun `l5a a result reaching an instance that asked nothing, or a destroyed one, starts nothing`() = runTest {
        val before = Launch(this)
        before.launcher.startLive()
        val code = before.lastCode()
        before.launcher.destroy()
        before.controller.destroy()
        // The destroyed launcher's own request is retired: its result settles nothing and refuses nothing.
        before.launcher.onPermissionsResult(code, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(emptyList(), before.refusals, "a destroyed launcher answers nothing")
        val after = Launch(this)
        after.launcher.onPermissionsResult(code, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(emptyList(), after.starts, "the new instance asked nothing, so nothing starts")
        assertEquals(emptyList(), before.starts)
        assertEquals(emptyList(), after.refusals)
    }

    @Test
    fun `l11 an interrupted or partial permission result is not a grant, and a later start proceeds`() = runTest {
        // An empty result is the platform's cancellation.
        val launch = Launch(this)
        launch.launcher.startLive()
        launch.launcher.onPermissionsResult(launch.lastCode(), emptyMap())
        runCurrent()
        assertEquals(listOf("the permission request was interrupted; nothing was granted"), launch.refusals)
        assertEquals(emptyList(), launch.starts)

        // A result that answers the camera but not the microphone.
        launch.launcher.startLive()
        assertEquals(both, launch.port.asks.last().second, "the interrupted request is asked again, whole")
        launch.launcher.onPermissionsResult(launch.lastCode(), mapOf(CapturePermission.Camera to true))
        runCurrent()
        assertEquals("the permission request was interrupted before the microphone was answered", launch.refusals.last())
        assertEquals(emptyList(), launch.starts, "a missing answer is never a grant")

        // Neither left an outstanding request behind: a later start asks and starts.
        launch.port.already.add(CapturePermission.Camera)
        launch.launcher.startLive()
        assertEquals(setOf(CapturePermission.Microphone), launch.port.asks.last().second)
        launch.launcher.onPermissionsResult(launch.lastCode(), mapOf(CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(1, launch.starts.size, "the later start proceeds")
        assertEquals(3, launch.port.asks.map { it.first }.toSet().size, "each request had its own code")
        launch.launcher.destroy()
        launch.controller.destroy()
        runCurrent()
    }

    @Test
    fun `l12 request codes continue across saved state, and refuse rather than wrap`() {
        val codes = RequestCodes.fresh()
        val first = checkNotNull(codes.issue())
        val second = checkNotNull(codes.issue())
        assertTrue(first >= RequestCodes.FIRST && second > first, "issued in order, never below the first")
        val restored = RequestCodes.restored(codes.saved())
        assertTrue(checkNotNull(restored.issue()) > second, "a recreated lineage continues past every code its predecessor issued")

        // The last code is issued once; after it, nothing.
        val last = RequestCodes.restored(RequestCodes.LAST)
        assertEquals(RequestCodes.LAST, last.issue())
        assertEquals(null, last.issue(), "exhausted, not wrapped")
        assertEquals(null, RequestCodes.restored(last.saved()).issue(), "and still exhausted after recreation")

        // A saved value this class never wrote issues nothing.
        for (bad in listOf(0, -1, Int.MIN_VALUE)) {
            assertEquals(null, RequestCodes.restored(bad).issue(), "saved $bad")
        }
    }

    @Test
    fun `l13 an exhausted issuer refuses a live start by name and asks nothing`() = runTest {
        val exhausted = RequestCodes.restored(RequestCodes.LAST).also { it.issue() }
        val launch = Launch(this, exhausted)
        launch.launcher.startLive()
        runCurrent()
        assertEquals(emptyList(), launch.port.asks)
        assertEquals(listOf("no permission request code remains for this screen"), launch.refusals)
        assertEquals(emptyList(), launch.starts)
    }

    @Test
    fun `l10 a live start or a permission result never stops a running broadcast, and is refused by name`() = runTest {
        // A synthetic broadcast is running: a live start asks nothing and stops nothing.
        val launch = Launch(this)
        val synthetic = mutableListOf<String>()
        launch.controller.toggle {
            try {
                awaitCancellation()
            } catch (stopped: CancellationException) {
                synthetic.add("cancelled")
                throw stopped
            }
        }
        runCurrent()
        launch.launcher.startLive()
        runCurrent()
        assertEquals(emptyList(), launch.port.asks, "nothing asked")
        assertEquals(listOf("a broadcast is already running; stop it first"), launch.refusals)
        assertEquals(emptyList(), synthetic, "and the running broadcast was not stopped")

        // A prompt is outstanding when the synthetic broadcast starts; its grant arrives after.
        val pending = Launch(this)
        pending.launcher.startLive()
        val code = pending.lastCode()
        val other = mutableListOf<String>()
        pending.controller.toggle {
            try {
                awaitCancellation()
            } catch (stopped: CancellationException) {
                other.add("cancelled")
                throw stopped
            }
        }
        runCurrent()
        pending.launcher.onPermissionsResult(code, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(emptyList(), pending.starts, "the grant starts nothing")
        assertEquals(emptyList(), other, "and is never an implicit Stop")
        assertEquals(listOf("a broadcast is already running; stop it first"), pending.refusals)
        launch.controller.destroy()
        pending.controller.destroy()
        runCurrent()
    }

    @Test
    fun `l14 the platform's result arrays are read as one answer per permission, or refused`() {
        val camera = "android.permission.CAMERA"
        val microphone = "android.permission.RECORD_AUDIO"
        assertEquals(
            PermissionReply.Answered(mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to false)),
            PermissionReplies.read(arrayOf(camera, microphone), intArrayOf(GRANTED, DENIED)),
        )
        assertEquals(PermissionReply.Answered(emptyMap()), PermissionReplies.read(emptyArray(), IntArray(0)), "the platform's cancellation stays empty")
        assertEquals(
            PermissionReply.Answered(mapOf(CapturePermission.Microphone to true)),
            PermissionReplies.read(arrayOf(microphone), intArrayOf(GRANTED)),
            "an answer for one permission names only that one",
        )
        class Bad(val label: String, val names: Array<String>, val grants: IntArray, val reason: String)
        for (bad in listOf(
            // First: without a length check this case still reads cleanly, so it fails by its own assertion.
            Bad("more grants", arrayOf(camera), intArrayOf(GRANTED, GRANTED), "the permission result has 1 name(s) but 2 grant result(s)"),
            Bad("more names", arrayOf(camera, microphone), intArrayOf(GRANTED), "the permission result has 2 name(s) but 1 grant result(s)"),
            Bad("names without grants", arrayOf(camera), IntArray(0), "the permission result has 1 name(s) but 0 grant result(s)"),
            Bad("duplicate", arrayOf(camera, camera), intArrayOf(DENIED, GRANTED), "the permission result answers android.permission.CAMERA twice"),
            Bad("unknown", arrayOf(camera, "android.permission.READ_CONTACTS"), intArrayOf(GRANTED, GRANTED), "the permission result names android.permission.READ_CONTACTS, which this sample never asks for"),
            Bad("unknown grant", arrayOf(microphone), intArrayOf(7), "the permission result answers android.permission.RECORD_AUDIO with 7, which is neither granted nor denied"),
        )) {
            assertEquals(PermissionReply.Malformed(bad.reason), PermissionReplies.read(bad.names, bad.grants), bad.label)
        }
    }

    @Test
    fun `l15 a malformed or incomplete reply settles its request as a named refusal, never a grant`() = runTest {
        val launch = Launch(this)
        launch.launcher.startLive()
        val code = launch.lastCode()
        // A malformed reply for another code is not this request's.
        launch.launcher.onPermissionsReply(code + 1, PermissionReply.Malformed("not ours"))
        assertEquals(emptyList(), launch.refusals)
        launch.launcher.onPermissionsReply(code, PermissionReplies.read(arrayOf("android.permission.CAMERA", "android.permission.CAMERA"), intArrayOf(GRANTED, GRANTED)))
        runCurrent()
        assertEquals(listOf("the permission result was malformed: the permission result answers android.permission.CAMERA twice"), launch.refusals)
        assertEquals(emptyList(), launch.starts, "a malformed reply is never a grant")
        // Its request is settled: the same code's grant, arriving later, starts nothing.
        launch.launcher.onPermissionsReply(code, PermissionReplies.read(arrayOf("android.permission.CAMERA", "android.permission.RECORD_AUDIO"), intArrayOf(GRANTED, GRANTED)))
        runCurrent()
        assertEquals(emptyList(), launch.starts)

        // Read from the arrays, a reply that answers only the camera leaves the microphone unanswered.
        launch.launcher.startLive()
        launch.launcher.onPermissionsReply(launch.lastCode(), PermissionReplies.read(arrayOf("android.permission.CAMERA"), intArrayOf(GRANTED)))
        runCurrent()
        assertEquals("the permission request was interrupted before the microphone was answered", launch.refusals.last())
        assertEquals(emptyList(), launch.starts)

        // And a well-formed grant, read the same way, starts the session.
        launch.launcher.startLive()
        launch.launcher.onPermissionsReply(launch.lastCode(), PermissionReplies.read(arrayOf("android.permission.CAMERA", "android.permission.RECORD_AUDIO"), intArrayOf(GRANTED, GRANTED)))
        runCurrent()
        assertEquals(1, launch.starts.size)
        launch.launcher.destroy()
        launch.controller.destroy()
        runCurrent()
    }

    @Test
    fun `l16 an invalid namespace is a named session outcome, never an exception in the UI callback`() = runTest {
        val runs = mutableListOf<Pair<String, Namespace>>()
        var entered = LiveInputs("moqt://192.0.2.7:14443", "studio/cam9")
        val launch =
            Launch(this, live = { start ->
                LiveSessions.work(entered, start) { url, ns, _, _ ->
                    runs.add(url to ns)
                    BroadcastOutcome.Unfinished("scripted", null, null)
                }
            })
        launch.port.already.addAll(both)
        // A valid input reaches the broadcast exactly as entered.
        launch.launcher.startLive()
        runCurrent()
        assertEquals(listOf("moqt://192.0.2.7:14443" to LiveSessions.parseNamespace("studio/cam9")), runs)
        assertTrue(SessionUpdate.Reported("not published: scripted") in launch.rendered)

        class Bad(val label: String, val namespace: String, val reason: String)
        for (bad in listOf(
            Bad("33 parts", (1..33).joinToString("/") { "p$it" }, "a track namespace carries at most 32 parts"),
            Bad("4097 bytes", "a".repeat(4097), ""),
        )) {
            entered = LiveInputs("moqt://192.0.2.7:14443", bad.namespace)
            launch.rendered.clear()
            // From the button.
            assertEquals(null, runCatching { launch.launcher.startLive() }.exceptionOrNull(), "${bad.label}: the UI callback raises nothing")
            runCurrent()
            val lines = launch.rendered.filterIsInstance<SessionUpdate.Reported>().map { it.line }
            assertTrue(lines.size == 1 && lines[0].startsWith("cannot publish live: the namespace is not valid: " + bad.reason), "${bad.label}: named, as a session line: $lines")
            assertEquals(SessionUpdate.Stopped, launch.rendered.last(), "${bad.label}: and the session ended")
        }
        // From a permission result.
        launch.port.already.clear()
        launch.launcher.startLive()
        assertEquals(null, runCatching { launch.launcher.onPermissionsResult(launch.lastCode(), mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true)) }.exceptionOrNull(), "the permission callback raises nothing")
        runCurrent()
        assertEquals(1, runs.size, "no invalid namespace reached a broadcast")
    }

    @Test
    fun `l17 the entered connection target survives recreation, and nothing resumes or starts on its own`() = runTest {
        val runs = mutableListOf<Pair<String, Namespace>>()
        fun screen(inputs: PublisherInputs, codes: RequestCodes) =
            Launch(this, codes, live = { start ->
                LiveSessions.work(inputs.live(), start) { url, ns, _, _ ->
                    runs.add(url to ns)
                    BroadcastOutcome.Unfinished("scripted", null, null)
                }
            })
        val custom = PublisherInputs(url = "moqt://192.0.2.7:14443", namespace = "studio/cam9", track = "program", frames = "120")
        val codes = RequestCodes.fresh()
        val before = screen(custom, codes)
        before.launcher.startLive()
        val outstanding = before.lastCode()
        // Recreation with the request outstanding: the predecessor saves its state and is destroyed.
        val saved = mutableMapOf<String, String>()
        custom.save { key, value -> saved[key] = value }
        val savedCodes = codes.saved()
        before.launcher.destroy()
        before.controller.destroy()
        val restored = PublisherInputs.restored { saved[it] }
        assertEquals(custom, restored, "the entered inputs, exactly")
        val after = screen(restored, RequestCodes.restored(savedCodes))
        runCurrent()
        assertEquals(emptyList(), after.port.asks, "nothing is asked again on its own")
        // The predecessor's outstanding result arrives: it neither resumes nor starts anything.
        after.launcher.onPermissionsResult(outstanding, mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(emptyList(), runs, "the old request does not resume")
        assertEquals(emptyList(), after.rendered, "and nothing started")
        // A new start uses the restored target.
        after.launcher.startLive()
        after.launcher.onPermissionsResult(after.lastCode(), mapOf(CapturePermission.Camera to true, CapturePermission.Microphone to true))
        runCurrent()
        assertEquals(listOf("moqt://192.0.2.7:14443" to LiveSessions.parseNamespace("studio/cam9")), runs, "the restored target, not the default")
        // Nothing saved is the default screen; a missing field is that field's default.
        assertEquals(PublisherInputs.DEFAULT, PublisherInputs.restored { null })
        assertEquals(PublisherInputs.DEFAULT.copy(url = custom.url), PublisherInputs.restored { key -> saved[key]?.takeIf { it == custom.url } })
    }

    @Test
    fun `l18 a late cleanup fact is kept after its session retired and after teardown, and never rendered into another session`() = runTest {
        val diagnostics = Diagnostics()
        val sinks = mutableMapOf<Long, ReportedLateCleanup>()
        val launch =
            Launch(this, live = { start ->
                { reporter ->
                    sinks[start.generation] = ReportedLateCleanup({ reporter.report(it) }, diagnostics)
                    awaitCancellation()
                }
            })
        launch.port.already.addAll(both)
        launch.launcher.startLive()
        runCurrent()
        val first = sinks.keys.single()
        launch.controller.toggle { }
        runCurrent()
        assertEquals(SessionUpdate.Stopped, launch.rendered.last(), "the first session has retired")
        launch.launcher.startLive()
        runCurrent()
        val second = sinks.keys.single { it != first }
        val mark = launch.rendered.size

        // The retired session's late close: kept, and not rendered into the running one.
        val failure = IllegalStateException("the encoder would not close")
        sinks.getValue(first).onLateClose(first, CaptureResource.VideoEncoder, failure)
        runCurrent()
        assertEquals(listOf(LateCleanupFact(first, CaptureResource.VideoEncoder, LateCleanupKind.LateClose, failure)), diagnostics.facts(), "kept out of band, with its generation, resource, kind and failure")
        assertSame(failure, diagnostics.facts().single().failure)
        assertEquals(emptyList(), launch.rendered.drop(mark), "no line of the retired session reaches the running one")

        // The running session's own fact is kept, and may also be its own line.
        sinks.getValue(second).onForeignDisposal(second, CaptureResource.Microphone, null)
        runCurrent()
        assertEquals(LateCleanupFact(second, CaptureResource.Microphone, LateCleanupKind.ForeignDisposal, null), diagnostics.facts().last())

        // After teardown: still kept, nothing rendered.
        launch.launcher.destroy()
        launch.controller.destroy()
        runCurrent()
        val rendered = launch.rendered.size
        sinks.getValue(second).onLateClose(second, CaptureResource.Camera, null)
        runCurrent()
        assertEquals(LateCleanupFact(second, CaptureResource.Camera, LateCleanupKind.LateClose, null), diagnostics.facts().last(), "kept after teardown")
        assertEquals(3, diagnostics.facts().size)
        assertEquals(rendered, launch.rendered.size, "and nothing rendered after teardown")
    }

    // -- the broadcast ------------------------------------------------------------------------------

    /** The platform's facts and capture kit, scripted. */
    private class Platform(val kit: CaptureKit?) : LivePlatform {
        var calibration: CalibrationOutcome = CalibrationOutcome.Measured(ClockCalibration(BASE_NANOS, UnixMicros(EPOCH), 10L))
        var camera: CaptureCapability = CaptureCapability.Available
        var audio: CaptureCapability = CaptureCapability.Available

        override fun calibrate() = calibration

        override fun cameraFacts() = camera

        override fun audioFacts() = audio

        /** The late cleanup sink the broadcast handed the kit. */
        var late: LateCleanupSink? = null

        override fun kit(late: LateCleanupSink): CaptureKit? {
            this.late = late
            return kit
        }
    }

    /** Late cleanup facts, kept as they arrive, from any thread. */
    private class Diagnostics : LateDiagnostics {
        private val lock = Any()
        private val kept = mutableListOf<LateCleanupFact>()

        override fun record(fact: LateCleanupFact) {
            synchronized(lock) { kept.add(fact) }
        }

        fun facts(): List<LateCleanupFact> = synchronized(lock) { kept.toList() }
    }

    /** The endpoint and sender, scripted: every call logged, the tracks and completion handed out by identity. */
    private class Endpoint(private val log: MutableList<String>) : LiveEndpoint<String, String, HostTrack> {
        /** The step that raises, and what it raises; a close's failure is logged first. */
        var failAt: String? = null
        var failure: Throwable? = null
        val tracks = ScriptedTracks()
        val completion = Completion()
        var config: EndpointConfig? = null
        var senderConfig: SenderConfig? = null

        override suspend fun connect(config: EndpointConfig): String {
            log.add("connect")
            if (failAt == "connect") throw checkNotNull(failure)
            this.config = config
            return "endpoint"
        }

        override suspend fun attach(endpoint: String, config: SenderConfig): String {
            log.add("attach")
            if (failAt == "attach") throw checkNotNull(failure)
            senderConfig = config
            return "sender"
        }

        override suspend fun awaitReady(sender: String) {
            log.add("ready")
            if (failAt == "ready") throw checkNotNull(failure)
        }

        override fun tracks(sender: String): TrackOperations<HostTrack> = tracks

        override fun completion(sender: String, endpoint: String): CompletionSink = completion

        override suspend fun closeSender(sender: String) {
            log.add("close sender")
            if (failAt == "close sender") throw checkNotNull(failure)
        }

        override suspend fun closeEndpoint(endpoint: String) {
            log.add("close endpoint")
            if (failAt == "close endpoint") throw checkNotNull(failure)
        }
    }

    /** Records the composition it is given and answers with a scripted run. */
    private class Composer(private val log: MutableList<String>, private val run: suspend () -> BroadcastOutcome) : LiveComposer {
        var composed: LiveComposition<*>? = null

        override fun <H : Any> compose(composition: LiveComposition<H>, report: (String) -> Unit): suspend () -> BroadcastOutcome {
            log.add("compose")
            composed = composition
            return {
                log.add("run")
                run()
            }
        }
    }

    private fun kit(): CaptureKit {
        val log = mutableListOf<String>()
        val video = TrackEncoder(VIDEO, HostEncoderBackend(log))
        val audio = TrackEncoder(AUDIO, HostEncoderBackend(log))
        val camera =
            object : CameraPort {
                override fun facts(): CameraFacts = error("no camera is opened here")

                override fun open(events: CameraEvents) = error("no camera is opened here")
            }
        val microphone = MicrophonePort { error("no microphone is opened here") }
        val dispatcher = HostDispatcher()
        val acquirer =
            SequencedAcquirer(
                WorkerStart { error("nothing starts here") },
                CameraPart(video, camera, LateLog()),
                AudioPart(audio, microphone, { _, _ -> null }, dispatcher, LateLog()),
            )
        return CaptureKit(dispatcher, video, audio, acquirer)
    }

    private val namespace = Namespace(listOf(NamespacePart("live".encodeToByteArray()), NamespacePart("cam1".encodeToByteArray())))

    private fun start(generation: Long = 7L) = LiveStart(generation, Granted(generation))

    @Test
    fun `l6 the live broadcast composes the platform's own capture kit, flow, clock and endpoint, by identity`() = runTest {
        val log = mutableListOf<String>()
        val kit = kit()
        val platform = Platform(kit)
        val endpoint = Endpoint(log)
        val composer = Composer(log) { BroadcastOutcome.Unfinished("scripted", null, null) }
        val diagnostics = Diagnostics()
        val live = start()
        val outcome = CameraBroadcast({ }, platform, endpoint, composer, diagnostics).broadcast("moqt://10.0.2.2:4443", namespace, live)
        assertEquals(BroadcastOutcome.Unfinished("scripted", null, null), outcome)
        val composed = checkNotNull(composer.composed)
        assertEquals(7L, composed.generation)
        assertSame(live.permissions, composed.permissions, "the generation's own permission flow")
        assertSame((platform.calibration as CalibrationOutcome.Measured).calibration, composed.calibration, "the measured calibration")
        assertSame(kit.acquirer, composed.environment.acquirer)
        assertSame(kit.dispatcher, composed.environment.dispatcher)
        assertSame(kit.video, composed.environment.video.lifetime)
        assertSame(kit.audio, composed.environment.audio.lifetime)
        assertSame(endpoint.tracks, composed.tracks, "the sender's own tracks")
        assertSame(endpoint.completion, composed.completion, "and its own completion")
        assertEquals(7L, composed.record.generation)
        val config = kit.audio.configuration as AudioEncoderConfiguration
        assertEquals(listOf(config.sampleRate, AudioEncoderConfiguration.FRAMES_PER_UNIT), listOf(composed.audioKeying.sampleRate, composed.audioKeying.framesPerUnit), "keyed on the configured PCM geometry")
        assertEquals(composed.origin, composed.audioKeying.originUs, "from the broadcast's own origin")
        // The kit and the composition share one late cleanup sink, and it keeps what it is told.
        val late = checkNotNull(platform.late)
        assertSame(late, composed.environment.late, "one late cleanup sink")
        val failure = IllegalStateException("the camera would not close")
        late.onLateClose(7L, CaptureResource.Camera, failure)
        assertEquals(listOf(LateCleanupFact(7L, CaptureResource.Camera, LateCleanupKind.LateClose, failure)), diagnostics.facts(), "the broadcast's late cleanup is kept out of band")
    }

    @Test
    fun `l7 a missing camera, an unavailable clock or an unusable calibration is named before anything connects`() = runTest {
        class Case(val label: String, val script: (Platform) -> Unit, val kit: Boolean, val reason: String)
        val cases =
            listOf(
                Case("no camera", {}, kit = false, "this device has no back camera"),
                Case("camera clock", { it.camera = CaptureCapability.Unavailable("the camera clock is not realtime") }, kit = true, "the camera clock is not realtime"),
                Case("audio clock", { it.audio = CaptureCapability.Unavailable("AudioRecord.getTimestamp needs API 24") }, kit = true, "AudioRecord.getTimestamp needs API 24"),
                Case("calibration", { it.calibration = CalibrationOutcome.Unusable("the bracket was too wide") }, kit = true, "the bracket was too wide"),
            )
        for (case in cases) {
            val log = mutableListOf<String>()
            val platform = Platform(if (case.kit) kit() else null).also(case.script)
            val reported = mutableListOf<String>()
            val outcome = CameraBroadcast({ reported.add(it) }, platform, Endpoint(log), Composer(log) { error("never run") }, LateDiagnostics { }).broadcast("moqt://10.0.2.2:4443", namespace, start())
            assertEquals(BroadcastOutcome.Unfinished(case.reason, null, null), outcome, case.label)
            assertEquals(emptyList(), log, "${case.label}: nothing connected, attached or composed")
            assertEquals(listOf("cannot publish live: ${case.reason}"), reported, case.label)
        }
    }

    @Test
    fun `l8 the endpoint is connected with the platform trust policy, and closed sender-first on every exit`() = runTest {
        val log = mutableListOf<String>()
        val endpoint = Endpoint(log)
        CameraBroadcast({ }, Platform(kit()), endpoint, Composer(log) { BroadcastOutcome.Unfinished("done", null, null) }, LateDiagnostics { })
            .broadcast("moqt://10.0.2.2:4443", namespace, start())
        assertEquals(EndpointConfig("moqt://10.0.2.2:4443"), endpoint.config, "the given URL, verified by the platform trust manager")
        assertSame(TrustPolicy.PlatformTrustManager, checkNotNull(endpoint.config).trustPolicy)
        assertEquals(SenderConfig(namespace, SendBackpressure.ReturnWouldBlock), endpoint.senderConfig, "never dropping on the service's behalf")
        assertEquals(listOf("connect", "attach", "ready", "compose", "run", "close sender", "close endpoint"), log)

        // A cancelled run still closes both, in order, and the cancellation leaves as itself.
        val stoppedLog = mutableListOf<String>()
        val stop = CancellationException("stopped")
        val escaped =
            kotlin.runCatching {
                CameraBroadcast({ }, Platform(kit()), Endpoint(stoppedLog), Composer(stoppedLog) { throw stop }, LateDiagnostics { }).broadcast("moqt://10.0.2.2:4443", namespace, start())
            }.exceptionOrNull()
        assertSame(stop, escaped)
        assertEquals(listOf("connect", "attach", "ready", "compose", "run", "close sender", "close endpoint"), stoppedLog)

        // A partial acquisition closes exactly what it acquired, in order, and composes nothing.
        class Arm(val at: String, val closes: List<String>)
        for (arm in listOf(
            Arm("connect", listOf("connect")),
            Arm("attach", listOf("connect", "attach", "close endpoint")),
            Arm("ready", listOf("connect", "attach", "ready", "close sender", "close endpoint")),
        )) {
            val armLog = mutableListOf<String>()
            val failing = Endpoint(armLog).also { it.failAt = arm.at; it.failure = IllegalStateException("${arm.at} failed") }
            val raised = kotlin.runCatching { CameraBroadcast({ }, Platform(kit()), failing, Composer(armLog) { error("never run") }, LateDiagnostics { }).broadcast("moqt://10.0.2.2:4443", namespace, start()) }.exceptionOrNull()
            assertSame(failing.failure, raised, "${arm.at}: the failure leaves as itself")
            assertEquals(arm.closes, armLog, "${arm.at}: exactly what was acquired is closed, once, and nothing is composed")
        }

        // A close that fails while a failure is leaving: every close is still attempted, and the primary carries it.
        val cleanupLog = mutableListOf<String>()
        val primary = IllegalStateException("the run failed")
        val closing = Endpoint(cleanupLog).also { it.failAt = "close sender"; it.failure = IllegalStateException("the sender would not close") }
        val leaving = kotlin.runCatching { CameraBroadcast({ }, Platform(kit()), closing, Composer(cleanupLog) { throw primary }, LateDiagnostics { }).broadcast("moqt://10.0.2.2:4443", namespace, start()) }.exceptionOrNull()
        assertSame(primary, leaving)
        assertEquals(1, primary.suppressed.count { it === closing.failure }, "carrying the close failure once")
        assertEquals(listOf("connect", "attach", "ready", "compose", "run", "close sender", "close endpoint"), cleanupLog)
    }

    @Test
    fun `l9 the production composer runs the accepted deferred composition over the kit's own acquirer`() = runTest {
        val log = mutableListOf<String>()
        val kit = kit()
        val composition =
            LiveComposition(
                generation = 7L,
                permissions = Granted(7L),
                cameraFacts = CaptureCapability.Available,
                audioFacts = CaptureCapability.Available,
                calibration = ClockCalibration(BASE_NANOS, UnixMicros(EPOCH), 10L),
                origin = ORIGIN,
                audioKeying = AUDIO.keying(ORIGIN),
                videoName = VIDEO_CONFIG.name,
                audioName = AUDIO_CONFIG.name,
                environment = CaptureEnvironment(kit.acquirer, kit.dispatcher, LateLog(), kit.video.trackCodec, kit.audio.trackCodec),
                tracks = ScriptedTracks(),
                completion = Completion(),
                record = CaptureDiscardRecord(7L),
                writeTimeout = WRITE_TIMEOUT,
                drainTimeout = DRAIN_TIMEOUT,
            )
        val run = composer.compose(composition) { log.add(it) }
        assertEquals(emptyList(), log, "composing reports nothing and starts nothing")
        // Only the real composition reaches the kit's own acquirer: its camera
        // part's check reads the camera's facts, which this kit refuses, and
        // the accepted coordinator names that refusal.
        assertEquals(
            BroadcastOutcome.Unfinished("the capture session refused to start: the camera's characteristics could not be read: no camera is opened here", null, null),
            run(),
        )
    }

    /** The composer l9 measures: the production one. */
    private val composer: LiveComposer = DeferredComposer

    private companion object {
        /** `PackageManager.PERMISSION_GRANTED` and `PERMISSION_DENIED`. */
        const val GRANTED = 0
        const val DENIED = -1
        val VIDEO: VideoEncoderConfiguration = (VideoEncoderConfiguration.of(1280, 720, 30, 2_000_000, 2_500_000uL, 2) as EncoderPlan.Planned<VideoEncoderConfiguration>).configuration
        val AUDIO: AudioEncoderConfiguration = (AudioEncoderConfiguration.of(48_000, 2, 96_000, 128_000uL) as EncoderPlan.Planned<AudioEncoderConfiguration>).configuration
    }
}
