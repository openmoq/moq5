package org.openmoq.moq5.samples.publisher

import android.app.Activity
import android.content.pm.PackageManager
import android.os.Bundle
import android.text.method.ScrollingMovementMethod
import android.view.Gravity
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.TextView
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel

/**
 * The publisher sample's UI: four inputs, two starts, one log.
 *
 * "Start synthetic" runs the finite example, [BroadcastSession], whose frames
 * are synthetic; "Start live camera + mic" asks for the CAMERA and
 * RECORD_AUDIO permissions through [LiveLauncher] and then runs a
 * [CameraBroadcast] over the back camera and the microphone. The live path
 * uses the relay URL and namespace; the track name and frame count are the
 * synthetic example's. While either runs, the first button is its Stop.
 *
 * Plain `android.widget` views for the same reason as the player: the sample
 * is about the MoQ surface, and all of the protocol lives in the sessions.
 *
 * Threading is explicit, and deliberately does NOT use `Dispatchers.Main`:
 * that dispatcher is declared by `kotlinx-coroutines-core` but IMPLEMENTED by
 * `kotlinx-coroutines-android`, which this sample does not depend on, so a
 * scope built on it would compile and lint cleanly here and then fail on a
 * device. The session runs on `Dispatchers.Default`; every input is read on
 * the UI thread BEFORE it starts, and every view mutation is marshalled back
 * through [runOnUiThread].
 */
class PublisherActivity : Activity() {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    /**
     * The Start/Stop lifecycle itself lives in [SessionController], so the
     * path this Activity actually takes can be driven by a test rather than
     * only by a finger on a button.
     */
    private val controller =
        SessionController(
            scope,
            UiDispatch { action -> runOnUiThread(action) },
        ) { update -> render(update) }

    private lateinit var codes: RequestCodes
    private lateinit var launcher: LiveLauncher

    private lateinit var url: EditText
    private lateinit var namespace: EditText
    private lateinit var track: EditText
    private lateinit var frames: EditText
    private lateinit var start: Button
    private lateinit var live: Button
    private lateinit var log: TextView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)

        // Restored from the predecessor's saved state, so a result for its
        // request can never carry a code this instance issues.
        codes = savedInstanceState?.let { RequestCodes.restored(it.getInt(REQUEST_CODES, 0)) } ?: RequestCodes.fresh()
        launcher = LiveLauncher(controller, permissionPort, codes, { line -> note(line) }) { admitted -> liveWork(admitted) }

        // The entered inputs survive recreation; nothing is asked or started again on their account.
        val entered = PublisherInputs.restored { key -> savedInstanceState?.getString(key) }
        url = field(entered.url)
        namespace = field(entered.namespace)
        track = field(entered.track)
        frames = field(entered.frames)
        log =
            TextView(this).apply {
                movementMethod = ScrollingMovementMethod()
                setPadding(PADDING, PADDING, PADDING, PADDING)
            }
        start =
            Button(this).apply {
                text = START
                setOnClickListener { toggle() }
            }
        live =
            Button(this).apply {
                text = START_LIVE
                setOnClickListener { launcher.startLive() }
            }

        val root =
            LinearLayout(this).apply {
                orientation = LinearLayout.VERTICAL
                gravity = Gravity.TOP
                setPadding(PADDING, PADDING, PADDING, PADDING)
                addView(label("Relay URL"))
                addView(url)
                addView(label("Namespace"))
                addView(namespace)
                addView(label("Track name"))
                addView(track)
                addView(label("Frames to publish"))
                addView(frames)
                addView(start)
                addView(live)
                addView(log, LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f))
            }
        setContentView(root)
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        outState.putInt(REQUEST_CODES, codes.saved())
        inputs().save { key, value -> outState.putString(key, value) }
    }

    override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<String>, grantResults: IntArray) {
        // The exact code, and the arrays read as they are: the launcher reads
        // each denial's rationale now, after the result.
        launcher.onPermissionsReply(requestCode, PermissionReplies.read(permissions, grantResults))
    }

    override fun onDestroy() {
        launcher.destroy()
        controller.destroy()
        scope.cancel()
        super.onDestroy()
    }

    /** The session's own updates, already on the UI thread. */
    private fun render(update: SessionUpdate) {
        when (update) {
            is SessionUpdate.Started -> {
                log.text = ""
                start.text = STOP
                live.isEnabled = false
            }
            is SessionUpdate.Reported -> {
                log.append(update.line)
                log.append("\n")
            }
            is SessionUpdate.Failed ->
                // Every public failure is typed; the sample shows the type,
                // because that is what an application would branch on.
                render(SessionUpdate.Reported(update.failure::class.simpleName + ": " + update.failure.message))
            is SessionUpdate.Stopped -> {
                start.text = START
                live.isEnabled = true
            }
        }
    }

    /** A launcher refusal, as a log line. */
    private fun note(line: String) {
        log.append(line)
        log.append("\n")
    }

    /** The Activity's side of the runtime permission surface. UI thread only. */
    private val permissionPort =
        object : PermissionPort {
            override fun ask(requestCode: Int, permissions: Set<CapturePermission>) {
                requestPermissions(permissions.map { PermissionReplies.platformName(it) }.toTypedArray(), requestCode)
            }

            override fun mayExplain(permission: CapturePermission): Boolean =
                shouldShowRequestPermissionRationale(PermissionReplies.platformName(permission))

            override fun granted(permission: CapturePermission): Boolean =
                checkSelfPermission(PermissionReplies.platformName(permission)) == PackageManager.PERMISSION_GRANTED
        }

    /** The screen's inputs as entered. UI thread only. */
    private fun inputs(): PublisherInputs =
        PublisherInputs(url.text.toString(), namespace.text.toString(), track.text.toString(), frames.text.toString())

    /** The admitted live start's work, its inputs snapshotted HERE on the UI thread. */
    private fun liveWork(admitted: LiveStart): suspend (SessionReporter) -> Unit {
        val platform = AndroidLivePlatform(applicationContext)
        return LiveSessions.work(inputs().live(), admitted) { inputUrl, inputNamespace, start, reporter ->
            CameraBroadcast({ line -> reporter.report(line) }, platform, AndroidLiveEndpoint, DeferredComposer, LogcatDiagnostics)
                .broadcast(inputUrl, inputNamespace, start)
        }
    }

    private fun toggle() {
        // Read every input HERE, on the UI thread, before any background work
        // exists: touching an EditText from the session's thread would be a
        // view accessed off the thread that created it.
        val inputUrl = url.text.toString().trim()
        val inputNamespace = namespace.text.toString().trim()
        val inputTrack = track.text.toString().trim()
        val inputFrames = frames.text.toString().trim().toIntOrNull() ?: PublisherInputs.DEFAULT.frames.toInt()
        controller.toggle { reporter ->
            val broadcast = BroadcastSession(report = { line -> reporter.report(line) })
            broadcast.broadcast(inputUrl, inputNamespace, inputTrack, inputFrames)
        }
    }


    private fun label(text: String): TextView = TextView(this).apply { this.text = text }

    private fun field(initial: String): EditText =
        EditText(this).apply {
            setText(initial)
            setSingleLine()
        }

    private companion object {
        const val START = "Start synthetic"
        const val START_LIVE = "Start live camera + mic"
        const val STOP = "Stop"
        const val REQUEST_CODES = "live.requestCodes"
        const val PADDING = 24
    }
}
