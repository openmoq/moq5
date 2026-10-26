package org.openmoq.moq5.samples.publisher

import android.Manifest
import android.content.pm.PackageManager
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.Namespace
import org.openmoq.moq5.NamespacePart
import org.openmoq.moq5.SendBackpressure
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.TrackName
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** The runtime-permission surface as the launcher uses it; the Activity implements it on the UI thread. */
internal interface PermissionPort {
    /** `Activity.requestPermissions` for exactly [permissions], under [requestCode]. */
    fun ask(requestCode: Int, permissions: Set<CapturePermission>)

    /** `shouldShowRequestPermissionRationale`, read AFTER a result. */
    fun mayExplain(permission: CapturePermission): Boolean

    /** `checkSelfPermission`: already granted, so not asked again. */
    fun granted(permission: CapturePermission): Boolean
}

/**
 * The permission request codes of one Activity lineage: every instance
 * recreated from saved state continues where its predecessor stopped, so a
 * result for a predecessor's request can never carry a code this instance
 * issued.
 *
 * The platform may recreate the activity stack before delivering a result
 * (`Activity.requestPermissions`), and it recreates it from the instance state
 * the predecessor saved. A code is issued only while the Activity is resumed,
 * and no Activity is destroyed for recreation or becomes killable before
 * `onSaveInstanceState` has run after that, so the [saved] value the Activity
 * puts in its state is past every code a delivered result can carry. A lineage
 * with no saved state has no predecessor whose results could arrive.
 *
 * Codes are never reused and never wrap: once [LAST] is issued, or when the
 * saved value is not one this class wrote, [issue] refuses.
 */
internal class RequestCodes private constructor(private var next: Int) {
    /** The next code, or null once none remains. */
    fun issue(): Int? {
        // Anything below FIRST -- including the exhausted marker, and a saved
        // value this class never wrote -- issues nothing.
        if (next < FIRST) return null
        val code = next
        next = if (code == LAST) EXHAUSTED else code + 1
        return code
    }

    /** What the Activity saves in `onSaveInstanceState`. */
    fun saved(): Int = next

    companion object {
        const val FIRST: Int = 1
        const val LAST: Int = Int.MAX_VALUE

        /** Saved once [LAST] has been issued; restoring it issues nothing. */
        private const val EXHAUSTED: Int = 0

        /** A lineage with no saved state. */
        fun fresh(): RequestCodes = RequestCodes(FIRST)

        /** A lineage recreated from what its predecessor [saved]. */
        fun restored(saved: Int): RequestCodes = RequestCodes(saved)
    }
}

/** One `onRequestPermissionsResult`, read from the platform's two arrays. */
internal sealed interface PermissionReply {
    /**
     * One answer per named permission, each named once. Empty is the
     * platform's cancellation; a permission that was asked but is absent here
     * was not answered.
     */
    data class Answered(val results: Map<CapturePermission, Boolean>) : PermissionReply

    /** Arrays that cannot be read as one answer per permission. Never a grant. */
    data class Malformed(val reason: String) : PermissionReply
}

/** Reads `onRequestPermissionsResult`'s arrays without letting a malformed pair become a map. */
internal object PermissionReplies {
    fun read(permissions: Array<String>, grantResults: IntArray): PermissionReply {
        if (permissions.size != grantResults.size) {
            return PermissionReply.Malformed(
                "the permission result has ${permissions.size} name(s) but ${grantResults.size} grant result(s)",
            )
        }
        val results = mutableMapOf<CapturePermission, Boolean>()
        for (index in permissions.indices) {
            val name = permissions[index]
            val permission =
                platformNames[name]
                    ?: return PermissionReply.Malformed("the permission result names $name, which this sample never asks for")
            if (permission in results) return PermissionReply.Malformed("the permission result answers $name twice")
            results[permission] =
                when (val grant = grantResults[index]) {
                    PackageManager.PERMISSION_GRANTED -> true
                    PackageManager.PERMISSION_DENIED -> false
                    else -> return PermissionReply.Malformed("the permission result answers $name with $grant, which is neither granted nor denied")
                }
        }
        return PermissionReply.Answered(results.toMap())
    }

    /** The platform name of each permission this sample asks for. */
    fun platformName(permission: CapturePermission): String =
        when (permission) {
            CapturePermission.Camera -> Manifest.permission.CAMERA
            CapturePermission.Microphone -> Manifest.permission.RECORD_AUDIO
        }

    private val platformNames: Map<String, CapturePermission> = CapturePermission.entries.associateBy { platformName(it) }
}

/** One admitted live start: the generation, and the permission flow that allowed it. */
internal class LiveStart(val generation: Long, val permissions: PermissionFlow)

/** What a live start reads from the screen, as entered, snapshotted on the UI thread. */
internal data class LiveInputs(val url: String, val namespace: String)

/** One live broadcast of an admitted start: [CameraBroadcast.broadcast] in production. */
internal fun interface LiveRun {
    suspend fun broadcast(url: String, namespace: Namespace, start: LiveStart, reporter: SessionReporter): BroadcastOutcome
}

/**
 * The admitted live start's session work.
 *
 * [work] is called on the UI thread with inputs already snapshotted there,
 * and does nothing else there: the namespace is parsed and validated inside
 * the returned work, so input the wire refuses is a named session line and
 * the session's end, never an exception out of a button or permission
 * callback.
 */
internal object LiveSessions {
    fun work(inputs: LiveInputs, admitted: LiveStart, run: LiveRun): suspend (SessionReporter) -> Unit =
        { reporter ->
            val namespace =
                try {
                    parseNamespace(inputs.namespace)
                } catch (invalid: IllegalArgumentException) {
                    reporter.report("cannot publish live: the namespace is not valid: " + invalid.message)
                    null
                }
            if (namespace != null) {
                when (val outcome = run.broadcast(inputs.url, namespace, admitted, reporter)) {
                    is BroadcastOutcome.Published -> reporter.report("published: both tracks ended and the terminal catalog reached the core")
                    is BroadcastOutcome.Unfinished -> reporter.report("not published: " + outcome.reason)
                }
            }
        }

    /** `a/b/c` as a namespace of three parts; empty parts are dropped. [Namespace] enforces the wire's bounds. */
    fun parseNamespace(text: String): Namespace =
        Namespace(
            text.split('/')
                .filter { it.isNotEmpty() }
                .map { NamespacePart(it.encodeToByteArray()) },
        )
}

/**
 * The live camera-and-microphone path's launcher, beside the synthetic
 * example's Start. UI thread only.
 *
 * One live start is one generation with its own [CapturePermissionFlow].
 * Permissions [PermissionPort.granted] already reports are settled as grants;
 * the rest are asked once, under a code from [codes], and at most one request
 * is outstanding. A result settles only the outstanding request whose code it
 * carries: a stale dialog's result, or a predecessor's after recreation,
 * settles nothing. Every result retires its request, so an interrupted,
 * partial or malformed one never leaves a request outstanding, and a later
 * start asks afresh.
 *
 * The session starts through [SessionController.startIfIdle], never
 * [SessionController.toggle]: a start or a permission answer that finds a
 * broadcast running or stopping is refused by name, and never stops it.
 */
internal class LiveLauncher(
    private val controller: SessionController,
    private val permissions: PermissionPort,
    private val codes: RequestCodes,
    private val refuse: (String) -> Unit,
    /**
     * Called on the UI thread when a start is admitted, so the caller reads
     * its inputs where they live; returns the session's work, which runs off
     * the UI thread.
     */
    private val live: (LiveStart) -> suspend (SessionReporter) -> Unit,
) {
    private class Request(val code: Int, val flow: CapturePermissionFlow, val asked: Set<CapturePermission>)

    private var generation = 0L
    private var outstanding: Request? = null
    private var destroyed = false

    fun startLive() {
        if (destroyed) return
        if (outstanding != null) {
            refuse(ALREADY_ASKING)
            return
        }
        if (!controller.idle()) {
            refuse(BUSY)
            return
        }
        val flow = CapturePermissionFlow(++generation)
        val held = CapturePermission.entries.filter { permissions.granted(it) }.toSet()
        flow.request(held)
        for (permission in held) flow.onResult(flow.generation, permission, PermissionFacts(granted = true, mayExplain = true))
        val wanted = CapturePermission.entries.toSet() - held
        if (wanted.isEmpty()) {
            begin(flow)
            return
        }
        val code = codes.issue()
        if (code == null) {
            flow.retire("no request code remains")
            refuse(NO_CODE)
            return
        }
        val asked = flow.request(wanted)
        outstanding = Request(code, flow, asked)
        permissions.ask(code, asked)
    }

    /**
     * One `onRequestPermissionsResult`. An empty [results] is the platform's
     * cancellation, and a permission that was asked but has no entry was not
     * answered: neither is a grant.
     */
    fun onPermissionsResult(requestCode: Int, results: Map<CapturePermission, Boolean>) {
        val request = settle(requestCode) ?: return
        if (results.isEmpty()) {
            request.flow.retire("interrupted")
            refuse(INTERRUPTED)
            return
        }
        val unanswered = CapturePermission.entries.firstOrNull { it in request.asked && it !in results }
        if (unanswered != null) {
            request.flow.retire("interrupted")
            refuse("the permission request was interrupted before the ${describe(unanswered)} was answered")
            return
        }
        for (permission in CapturePermission.entries) {
            if (permission !in request.asked) continue
            val granted = results.getValue(permission)
            // The rationale is read NOW, after the result: before it, it answers a different question.
            val facts = PermissionFacts(granted, mayExplain = granted || permissions.mayExplain(permission))
            request.flow.onResult(request.flow.generation, permission, facts)
        }
        when (request.flow.readiness()) {
            CaptureReadiness.Ready -> begin(request.flow)
            else -> refuse(refusalOf(request.flow))
        }
    }

    /** The Activity's entry: a [PermissionReply] read from the platform's arrays, under the exact request code. */
    fun onPermissionsReply(requestCode: Int, reply: PermissionReply) {
        when (reply) {
            is PermissionReply.Answered -> onPermissionsResult(requestCode, reply.results)
            is PermissionReply.Malformed -> {
                val request = settle(requestCode) ?: return
                request.flow.retire("malformed")
                refuse("the permission result was malformed: ${reply.reason}")
            }
        }
    }

    /** Activity teardown: the outstanding request, if any, is retired and its result will settle nothing. */
    fun destroy() {
        destroyed = true
        outstanding?.flow?.retire("the launcher was destroyed")
        outstanding = null
    }

    /** The outstanding request [requestCode] names, now settled; null for any other code. */
    private fun settle(requestCode: Int): Request? {
        if (destroyed) return null
        val request = outstanding ?: return null
        if (request.code != requestCode) return null
        outstanding = null
        return request
    }

    private fun begin(flow: CapturePermissionFlow) {
        if (!controller.startIfIdle(live(LiveStart(flow.generation, flow)))) {
            flow.retire("a broadcast is already running")
            refuse(BUSY)
        }
    }

    private fun refusalOf(flow: CapturePermissionFlow): String {
        for (permission in CapturePermission.entries) {
            when (flow.stateOf(permission)) {
                PermissionState.PermanentlyDenied -> return "the ${describe(permission)} permission was denied permanently; allow it in Settings"
                PermissionState.Denied -> return "the ${describe(permission)} permission was denied"
                else -> Unit
            }
        }
        return "the permissions were not granted"
    }

    private fun describe(permission: CapturePermission): String =
        when (permission) {
            CapturePermission.Camera -> "camera"
            CapturePermission.Microphone -> "microphone"
        }

    private companion object {
        const val BUSY = "a broadcast is already running; stop it first"
        const val ALREADY_ASKING = "the permission request is already on screen"
        const val NO_CODE = "no permission request code remains for this screen"
        const val INTERRUPTED = "the permission request was interrupted; nothing was granted"
    }
}

/** What `PlatformCapture.of` builds: one owned worker, both encoders, the one acquirer. It opens nothing. */
internal class CaptureKit(
    val dispatcher: CallbackDispatcher,
    val video: TrackEncoder,
    val audio: TrackEncoder,
    val acquirer: SequencedAcquirer,
)

/** What the live session asks of the platform before anything connects. */
internal interface LivePlatform {
    fun calibrate(): CalibrationOutcome

    fun cameraFacts(): CaptureCapability

    fun audioFacts(): CaptureCapability

    /** The capture kit, or null when the device has no back camera. */
    fun kit(late: LateCleanupSink): CaptureKit?
}

/** The library's endpoint and sender, as the live session drives them. */
internal interface LiveEndpoint<E : Any, S : Any, H : Any> {
    suspend fun connect(config: EndpointConfig): E

    suspend fun attach(endpoint: E, config: SenderConfig): S

    suspend fun awaitReady(sender: S)

    fun tracks(sender: S): TrackOperations<H>

    fun completion(sender: S, endpoint: E): CompletionSink

    suspend fun closeSender(sender: S)

    suspend fun closeEndpoint(endpoint: E)
}

/** Everything the deferred composition is given for one live broadcast. */
internal class LiveComposition<H : Any>(
    val generation: Long,
    val permissions: PermissionFlow,
    val cameraFacts: CaptureCapability,
    val audioFacts: CaptureCapability,
    val calibration: ClockCalibration,
    val origin: Long,
    val audioKeying: AudioKeying,
    val videoName: TrackName,
    val audioName: TrackName,
    val environment: CaptureEnvironment,
    val tracks: TrackOperations<H>,
    val completion: CompletionSink,
    val record: CaptureDiscardRecord,
    val writeTimeout: Duration,
    val drainTimeout: Duration,
)

/** The deferred composition as the live session calls it; [composeDeferredBroadcast] in production. */
internal interface LiveComposer {
    fun <H : Any> compose(composition: LiveComposition<H>, report: (String) -> Unit): suspend () -> BroadcastOutcome
}

/**
 * One live camera-and-microphone broadcast over the library's endpoint.
 *
 * Nothing connects until the platform has answered: the camera's and the
 * microphone's clocks can carry an epoch capture instant, the capture clock
 * is calibrated, and the device has a back camera. Each refusal is a named
 * [BroadcastOutcome.Unfinished], reported as it is.
 *
 * Then [broadcastLifetime] owns the endpoint from `connect` and the sender
 * from `attach`: the sender is awaited ready, the broadcast is composed over
 * the sender's own tracks and completion and run, and on EVERY exit the
 * sender is closed before the endpoint, each once and each attempted, with
 * whatever ended the run leaving as itself. The endpoint is configured with
 * [EndpointConfig]'s default trust -- the platform trust manager -- and the
 * sender never drops on the service's behalf.
 *
 * Stop is cancellation of the calling coroutine: it reaches the accepted
 * coordinator's own ending first, and these closes after it.
 */
internal class CameraBroadcast<E : Any, S : Any, H : Any>(
    private val report: (String) -> Unit,
    private val platform: LivePlatform,
    private val endpoint: LiveEndpoint<E, S, H>,
    private val composer: LiveComposer,
    private val diagnostics: LateDiagnostics,
) {
    suspend fun broadcast(url: String, namespace: Namespace, start: LiveStart): BroadcastOutcome {
        val cameraFacts = platform.cameraFacts()
        if (cameraFacts is CaptureCapability.Unavailable) return refused(cameraFacts.reason)
        val audioFacts = platform.audioFacts()
        if (audioFacts is CaptureCapability.Unavailable) return refused(audioFacts.reason)
        val calibration =
            when (val measured = platform.calibrate()) {
                is CalibrationOutcome.Measured -> measured.calibration
                is CalibrationOutcome.Unusable -> return refused(measured.reason)
            }
        // Built last, so a refusal above leaves nothing to dispose of; it opens nothing itself.
        val late = ReportedLateCleanup(report, diagnostics)
        val kit = platform.kit(late) ?: return refused(NO_BACK_CAMERA)
        val pcm = kit.audio.configuration as AudioEncoderConfiguration

        var outcome: BroadcastOutcome? = null
        report("connecting to $url")
        broadcastLifetime(
            connect = { endpoint.connect(EndpointConfig(url)) },
            describe = { },
            attach = { connected -> endpoint.attach(connected, SenderConfig(namespace, SendBackpressure.ReturnWouldBlock)) },
            run = { connected, sender ->
                report("waiting for the namespace to be accepted")
                endpoint.awaitReady(sender)
                report("ready; starting the camera and microphone")
                val run =
                    composer.compose(
                        LiveComposition(
                            generation = start.generation,
                            permissions = start.permissions,
                            cameraFacts = cameraFacts,
                            audioFacts = audioFacts,
                            calibration = calibration,
                            origin = ORIGIN,
                            audioKeying = pcm.keying(ORIGIN),
                            videoName = VIDEO_TRACK,
                            audioName = AUDIO_TRACK,
                            environment = CaptureEnvironment(kit.acquirer, kit.dispatcher, late, kit.video.trackCodec, kit.audio.trackCodec),
                            tracks = endpoint.tracks(sender),
                            completion = endpoint.completion(sender, connected),
                            record = CaptureDiscardRecord(start.generation),
                            writeTimeout = WRITE_TIMEOUT,
                            drainTimeout = DRAIN_TIMEOUT,
                        ),
                        report,
                    )
                outcome = run()
            },
            closeChild = { sender -> endpoint.closeSender(sender) },
            closeEndpoint = { connected -> endpoint.closeEndpoint(connected) },
        )
        return checkNotNull(outcome) { "the broadcast returned without an outcome" }
    }

    private fun refused(reason: String): BroadcastOutcome {
        report("cannot publish live: $reason")
        return BroadcastOutcome.Unfinished(reason, null, null)
    }

    private companion object {
        const val NO_BACK_CAMERA = "this device has no back camera"

        /** The broadcast's own origin: every track's timeline starts here. */
        const val ORIGIN = 0L
        val VIDEO_TRACK = TrackName("video".encodeToByteArray())
        val AUDIO_TRACK = TrackName("audio".encodeToByteArray())
        val WRITE_TIMEOUT = 1.seconds
        val DRAIN_TIMEOUT = 5.seconds
    }
}

/** Which late cleanup fact a [LateCleanupFact] is. */
internal enum class LateCleanupKind { LateClose, ForeignDisposal }

/** One late cleanup fact: the generation it belongs to, the resource, which fact, and the close's failure if any. */
internal data class LateCleanupFact(
    val generation: Long,
    val resource: CaptureResource,
    val kind: LateCleanupKind,
    val failure: Exception?,
)

/** Where late cleanup facts are kept. Called from any thread. */
internal fun interface LateDiagnostics {
    fun record(fact: LateCleanupFact)
}

/**
 * Late cleanup facts: they arrive after the resource's close has returned, on
 * whichever thread observed them, and are only ever told, never acted on here.
 *
 * Each fact is kept in [diagnostics] FIRST, which outlives the session and the
 * screen; a fact that arrives after its session retired is still kept there,
 * under its own generation. It is then also offered to [report], the session's
 * own generation-bound reporter, which renders it only while that session is
 * the current one and never into another.
 */
internal class ReportedLateCleanup(
    private val report: (String) -> Unit,
    private val diagnostics: LateDiagnostics,
) : LateCleanupSink {
    override fun onLateClose(generation: Long, resource: CaptureResource, failure: Exception?) {
        diagnostics.record(LateCleanupFact(generation, resource, LateCleanupKind.LateClose, failure))
        report("late close of the $resource" + (failure?.let { ": " + it.message } ?: ""))
    }

    override fun onForeignDisposal(generation: Long, resource: CaptureResource, failure: Exception?) {
        diagnostics.record(LateCleanupFact(generation, resource, LateCleanupKind.ForeignDisposal, failure))
        report("disposed of an unowned $resource" + (failure?.let { ": " + it.message } ?: ""))
    }
}

/**
 * The production composer: [composeDeferredBroadcast], unchanged, with this
 * sample's declared bounds. They bound what one live broadcast will hold; they
 * certify nothing about an encoder.
 */
internal object DeferredComposer : LiveComposer {
    override fun <H : Any> compose(composition: LiveComposition<H>, report: (String) -> Unit): suspend () -> BroadcastOutcome =
        composeDeferredBroadcast(
            generation = composition.generation,
            permissions = composition.permissions,
            cameraFacts = composition.cameraFacts,
            audioFacts = composition.audioFacts,
            calibration = composition.calibration,
            origin = composition.origin,
            audioKeying = composition.audioKeying,
            capacity = CAPACITY,
            maxOutputBytes = MAX_OUTPUT_BYTES,
            maxNalUnits = MAX_NAL_UNITS,
            videoName = composition.videoName,
            audioName = composition.audioName,
            formatBounds = FORMAT_BOUNDS,
            formatBudget = FORMAT_BUDGET,
            environment = composition.environment,
            tracks = composition.tracks,
            completion = composition.completion,
            record = composition.record,
            writeTimeout = composition.writeTimeout,
            drainTimeout = composition.drainTimeout,
            report = report,
        ).run

    /** Captured units awaiting their encoded output, per track. */
    private const val CAPACITY = 8

    /** One access unit's bytes: well above a 720p keyframe at the configured bitrate. */
    private const val MAX_OUTPUT_BYTES = 1_048_576

    /** NAL units in one access unit: parameter sets, SEI and a multi-slice picture. */
    private const val MAX_NAL_UNITS = 64

    /** The codec configuration each format carries: SPS/PPS for video, the AudioSpecificConfig for audio. */
    private val FORMAT_BOUNDS = ConfigBounds(maxUnits = 4, maxUnitBytes = 1024, maxTotalBytes = 2048)

    /** How long each track's format may take to arrive once capture starts. */
    private val FORMAT_BUDGET = 5.seconds
}
