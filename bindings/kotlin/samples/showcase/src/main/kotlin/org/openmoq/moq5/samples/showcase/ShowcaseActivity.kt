package org.openmoq.moq5.samples.showcase

import android.app.PictureInPictureParams
import android.content.Intent
import android.content.pm.PackageManager
import android.content.res.Configuration
import android.graphics.Rect
import android.os.Build
import android.os.Bundle
import android.util.Rational
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.viewModels
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import org.openmoq.moq5.samples.playback.VideoTrackInfo
import org.openmoq.moq5.samples.showcase.ui.ShowcaseApp
import org.openmoq.moq5.samples.showcase.ui.ShowcaseTheme

/**
 * The one activity. It hands everything with state to [ShowcaseViewModel]
 * and keeps the platform's side: links, picture-in-picture, audio focus and
 * the rule that leaving the app stops playback.
 *
 * Lifecycle, as a group:
 *  * rotation, resizing and dark mode are handled in place (see the
 *    manifest), so the video surface and the session carry on;
 *  * going Home while a session runs enters picture-in-picture where the
 *    device supports it (automatically from Android 12, from the leave hint
 *    before), and the session carries on in the small window;
 *  * anything that stops the activity -- leaving without picture-in-picture,
 *    closing the picture-in-picture window, the screen turning off -- stops
 *    the session. There is no background playback;
 *  * audio focus is the model's: a session starts only once focus is
 *    granted, and losing it or noisy output stops the session
 *    ([ShowcaseCore], [AudioPolicy]). Play rejoins the live broadcast;
 *    nothing was buffered while stopped.
 */
class ShowcaseActivity : ComponentActivity() {
    private val model: ShowcaseViewModel by viewModels()
    private val inPip = mutableStateOf(false)

    /** Where the video is on screen: the picture-in-picture transition starts from it. */
    private val videoBounds = mutableStateOf<Rect?>(null)
    private val pipSupported by lazy { packageManager.hasSystemFeature(PackageManager.FEATURE_PICTURE_IN_PICTURE) }

    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)
        if (savedInstanceState == null) openFrom(intent)
        setContent {
            val ui by model.ui.collectAsStateWithLifecycle()
            val player = ui.player
            val running = player?.running == true
            val video = player?.video
            val bounds = videoBounds.value
            LaunchedEffect(running, video, bounds) {
                if (pipSupported) setPictureInPictureParams(pipParams(video, running))
            }
            ShowcaseTheme {
                ShowcaseApp(
                    ui = ui,
                    model = model,
                    inPip = inPip.value,
                    pipSupported = pipSupported,
                    onPip = { enterPip(video) },
                    onVideoBounds = { if (videoBounds.value != it) videoBounds.value = it },
                )
            }
        }
    }

    override fun onNewIntent(intent: Intent) {
        super.onNewIntent(intent)
        setIntent(intent)
        openFrom(intent)
    }

    override fun onUserLeaveHint() {
        super.onUserLeaveHint()
        // From Android 12 the platform enters picture-in-picture itself (autoEnterEnabled).
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.S && model.ui.value.player?.running == true) {
            enterPip(model.ui.value.player?.video)
        }
    }

    override fun onPictureInPictureModeChanged(isInPictureInPictureMode: Boolean, newConfig: Configuration) {
        super.onPictureInPictureModeChanged(isInPictureInPictureMode, newConfig)
        inPip.value = isInPictureInPictureMode
    }

    override fun onStop() {
        super.onStop()
        if (!isChangingConfigurations) model.stop()
    }

    private fun openFrom(intent: Intent?) {
        intent ?: return
        intent.dataString?.let(::parseWatchLink)?.let { model.open(it); return }
        val url = intent.getStringExtra(EXTRA_RELAY_URL)
        val namespace = intent.getStringExtra(EXTRA_NAMESPACE)
        if (url != null && namespace != null && targetProblem(url, namespace) == null) model.watch(WatchTarget(url, namespace))
    }

    private fun enterPip(video: VideoTrackInfo?) {
        if (pipSupported) enterPictureInPictureMode(pipParams(video, model.ui.value.player?.running == true))
    }

    private fun pipParams(video: VideoTrackInfo?, running: Boolean): PictureInPictureParams {
        val builder = PictureInPictureParams.Builder().setAspectRatio(pipRatio(video))
        videoBounds.value?.let { builder.setSourceRectHint(it) }
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            builder.setAutoEnterEnabled(running)
            builder.setSeamlessResizeEnabled(true)
        }
        return builder.build()
    }

    /** The video's shape, within the 1:2.39 .. 2.39:1 the platform accepts; 16:9 before any video. */
    private fun pipRatio(video: VideoTrackInfo?): Rational {
        if (video == null || video.width <= 0 || video.height <= 0) return Rational(16, 9)
        val ratio = video.width.toDouble() / video.height
        return when {
            ratio > MAX_RATIO -> Rational(239, 100)
            ratio < 1 / MAX_RATIO -> Rational(100, 239)
            else -> Rational(video.width, video.height)
        }
    }

    private companion object {
        const val MAX_RATIO = 2.39
        const val EXTRA_RELAY_URL = "org.openmoq.moq5.samples.showcase.RELAY_URL"
        const val EXTRA_NAMESPACE = "org.openmoq.moq5.samples.showcase.NAMESPACE"
    }
}
