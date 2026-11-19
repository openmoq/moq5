package org.openmoq.moq5.samples.player

import android.app.Activity
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.ApplicationInfo
import android.graphics.SurfaceTexture
import android.os.Build
import android.os.Bundle
import android.os.Handler
import android.os.HandlerThread
import android.os.Looper
import android.text.method.ScrollingMovementMethod
import android.view.Gravity
import android.view.TextureView
import android.view.ViewGroup
import android.widget.Button
import android.widget.EditText
import android.widget.LinearLayout
import android.widget.ScrollView
import android.widget.TextView
import org.openmoq.moq5.samples.playback.AndroidPlayback
import org.openmoq.moq5.samples.playback.DiagnosticsListenerOwner
import org.openmoq.moq5.samples.playback.PlaybackCapture
import org.openmoq.moq5.samples.playback.PlaybackDiagnostics
import org.openmoq.moq5.samples.playback.PlaybackRequest
import org.openmoq.moq5.samples.playback.PlaybackTrace
import org.openmoq.moq5.samples.playback.SessionUpdate
import org.openmoq.moq5.samples.playback.UiDispatch

/** A small service-tier player: public receiver, owned decoder thread, and TextureView output. */
class PlayerActivity : Activity() {

    private var diagnostics: DiagnosticsListenerOwner? = null

    /**
     * The shared engine's screen-level owner: display, Start/Stop lifecycle and,
     * when the launch asks for it, the acceptance observer. Created in
     * [onCreate], once the launch flags are known.
     */
    private lateinit var playback: AndroidPlayback

    private lateinit var url: EditText
    private lateinit var namespace: EditText
    private lateinit var budget: EditText
    private lateinit var start: Button
    private lateinit var log: TextView
    private lateinit var preview: TextureView

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        val flags = playerFlags(intent::getBooleanExtra)
        PlaybackTrace.enabled = flags.trace
        val debuggable = (applicationInfo.flags and ApplicationInfo.FLAG_DEBUGGABLE) != 0
        if (PlaybackDiagnostics.armed(debuggable, flags.trace, Build.VERSION.SDK_INT)) {
            armDiagnostics()
        } else if (flags.trace) {
            PlaybackTrace.mark { "diagnostics-listener unarmed: debuggable=$debuggable sdk=${Build.VERSION.SDK_INT}" }
        }
        // The real dispatch boundary: produced on the session's thread,
        // delivered on the UI thread.
        playback = AndroidPlayback(UiDispatch { action -> runOnUiThread(action) }, flags.observe) { update -> render(update) }

        val inputs = playerInputs(intent::getStringExtra)
        url = field(inputs.url)
        namespace = field(inputs.namespace)
        budget = field(inputs.budget)
        log =
            TextView(this).apply {
                movementMethod = ScrollingMovementMethod()
                setPadding(PADDING, PADDING, PADDING, PADDING)
            }
        start =
            Button(this).apply {
                text = START
                isEnabled = false
                setOnClickListener { toggle() }
            }
        preview =
            object : TextureView(this) {
                override fun onMeasure(widthMeasureSpec: Int, heightMeasureSpec: Int) {
                    val width = MeasureSpec.getSize(widthMeasureSpec)
                    val height = minOf(width * 9 / 16, resources.displayMetrics.heightPixels / 2)
                    super.onMeasure(widthMeasureSpec, MeasureSpec.makeMeasureSpec(height, MeasureSpec.EXACTLY))
                }
            }.apply {
                surfaceTextureListener = object : TextureView.SurfaceTextureListener {
                    override fun onSurfaceTextureAvailable(surface: SurfaceTexture, width: Int, height: Int) {
                        playback.surfaceAvailable(surface)
                        start.isEnabled = true
                    }

                    override fun onSurfaceTextureSizeChanged(surface: SurfaceTexture, width: Int, height: Int) = Unit

                    override fun onSurfaceTextureDestroyed(surface: SurfaceTexture): Boolean {
                        start.isEnabled = false
                        return playback.surfaceDestroyed(surface)
                    }

                    override fun onSurfaceTextureUpdated(surface: SurfaceTexture) {
                        PlaybackTrace.mark { "texture updated ts=${surface.timestamp}" }
                        playback.frameShown(surface) { capture() }
                    }
                }
            }

        val root =
            LinearLayout(this).apply {
                orientation = LinearLayout.VERTICAL
                gravity = Gravity.TOP
                setPadding(PADDING, PADDING, PADDING, PADDING)
                addView(preview, LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, ViewGroup.LayoutParams.WRAP_CONTENT))
                addView(ScrollView(this@PlayerActivity).apply {
                    addView(LinearLayout(this@PlayerActivity).apply {
                        orientation = LinearLayout.VERTICAL
                        addView(label("Relay URL"))
                        addView(url)
                        addView(label("Namespace"))
                        addView(namespace)
                        addView(label("Objects to receive"))
                        addView(budget)
                        addView(start)
                        addView(log)
                    })
                }, LinearLayout.LayoutParams(ViewGroup.LayoutParams.MATCH_PARENT, 0, 1f))
            }
        setContentView(root)
    }

    /** A cancelled session still tears its endpoint down: the work is in a `finally`. */
    /**
     * Diagnostic-only: the harness's one stats request, delivered on an owned background thread so a
     * blocked main thread cannot hold it. The listener and its thread are one-shot (see
     * [DiagnosticsListenerOwner]); the answer is computed on [scope], never on either thread.
     */
    private fun armDiagnostics() {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.TIRAMISU) return
        val thread = HandlerThread("moq5-player-diagnostics")
        lateinit var owner: DiagnosticsListenerOwner
        val listener = object : BroadcastReceiver() {
            override fun onReceive(context: Context, intent: Intent) {
                PlaybackTrace.mark { "stats-callback thread=${Thread.currentThread().name} main=${Looper.myLooper() == Looper.getMainLooper()}" }
                owner.delivered(intent.action) { playback.requestDiagnostics() }
            }
        }
        owner = DiagnosticsListenerOwner(
            startThread = { thread.start() },
            register = {
                registerReceiver(listener, IntentFilter(PlaybackDiagnostics.ACTION), null, Handler(thread.looper), RECEIVER_EXPORTED)
            },
            unregister = { unregisterReceiver(listener) },
            quitThread = { thread.quitSafely() },
        )
        if (owner.open()) {
            diagnostics = owner
            PlaybackTrace.mark { "diagnostics-listener armed thread=${thread.name}" }
        }
    }

    override fun onDestroy() {
        diagnostics?.retire()
        diagnostics = null
        playback.destroy()
        super.onDestroy()
    }

    /** The session's own updates, already on the UI thread. */
    private fun render(update: SessionUpdate) {
        when (update) {
            is SessionUpdate.Started -> {
                log.text = ""
                start.text = STOP
            }
            is SessionUpdate.Reported -> {
                log.append(update.line)
                log.append("\n")
            }
            is SessionUpdate.Failed ->
                // Every public failure is typed; the sample shows the type,
                // because that is what an application would branch on.
                render(SessionUpdate.Reported(update.failure::class.simpleName + ": " + update.failure.message))
            // The typed facts repeat what the report lines already say here.
            is SessionUpdate.Event -> Unit
            is SessionUpdate.Stopped -> {
                start.text = START
                start.isEnabled = preview.isAvailable
            }
        }
    }

    private fun toggle() {
        // Read every input HERE, on the UI thread, before any background work
        // exists: touching an EditText from the session's thread would be a
        // view accessed off the thread that created it.
        val inputUrl = url.text.toString().trim()
        val inputNamespace = namespace.text.toString().trim()
        val inputBudget = budget.text.toString().trim().toIntOrNull() ?: DEFAULT_BUDGET.toInt()
        playback.toggle(PlaybackRequest(inputUrl, inputNamespace, inputBudget))
    }

    /** The displayed frame, downscaled for the marker reader; null when the view has none. */
    private fun capture(): PlaybackCapture? {
        val bitmap = preview.getBitmap(CAPTURE_WIDTH, CAPTURE_HEIGHT) ?: return null
        try {
            val pixels = IntArray(bitmap.width * bitmap.height)
            bitmap.getPixels(pixels, 0, bitmap.width, 0, 0, bitmap.width, bitmap.height)
            return PlaybackCapture.fromArgb(pixels, bitmap.width, bitmap.height)
        } finally {
            bitmap.recycle()
        }
    }


    private fun label(text: String): TextView = TextView(this).apply { this.text = text }

    private fun field(initial: String): EditText =
        EditText(this).apply {
            setText(initial)
            setSingleLine()
        }

    private companion object {
        const val START = "Start"
        const val STOP = "Stop"
        const val PADDING = 24
        const val CAPTURE_WIDTH = 64
        const val CAPTURE_HEIGHT = 36
    }
}
