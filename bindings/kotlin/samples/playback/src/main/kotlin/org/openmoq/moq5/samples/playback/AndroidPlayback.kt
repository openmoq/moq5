package org.openmoq.moq5.samples.playback

import android.graphics.SurfaceTexture
import android.util.Log
import android.view.Surface
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import org.openmoq.moq5.StartMode
import org.openmoq.moq5.VersionOffer
import kotlin.time.Duration.Companion.seconds

/**
 * What to play: the relay at [url], the broadcast [namespace] (slash-separated
 * parts), at most [objectBudget] objects, the drafts to offer and where each
 * track starts. With [stats], the session reports receiver statistics about
 * once a second as [PlaybackEvent.Stats].
 */
public class PlaybackRequest(
    public val url: String,
    public val namespace: String,
    public val objectBudget: Int = Int.MAX_VALUE,
    public val versions: VersionOffer = VersionOffer.Automatic,
    public val start: StartMode = StartMode.Current,
    public val stats: Boolean = false,
) {
    init { require(objectBudget > 0) { "object budget must be positive" } }
}

/**
 * Playback for one Android screen: the display, the Start/Stop lifecycle and,
 * when [observe] asks for it, the presentation observer the supervised
 * acceptance run reads -- wired as an application needs them.
 *
 * The owner forwards its `TextureView`'s texture callbacks and calls
 * [toggle], [stop] and finally [destroy], all on its UI thread. Session work
 * runs on a background scope; [onUpdate] arrives through [ui], on the UI
 * thread.
 */
public class AndroidPlayback(
    ui: UiDispatch,
    observe: Boolean = false,
    onUpdate: (SessionUpdate) -> Unit,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val display = PlaybackDisplayRegistry<SurfaceTexture, Surface>(
        createSurface = { Surface(it) },
        releaseSurface = { it.release() },
        releaseTransferredTexture = { it.release() },
    )
    private val presentation = presentationSeatFor(observe, display, System::nanoTime)
    private val controller = SessionController(scope, ui, onUpdate)

    /** The view's texture became available: playback can start. */
    public fun surfaceAvailable(texture: SurfaceTexture) {
        display.available(texture)
    }

    /**
     * The view's texture is going away: any session stops. Returns what
     * `onSurfaceTextureDestroyed` must return -- false while playback still
     * owns the texture and releases it itself.
     */
    public fun surfaceDestroyed(texture: SurfaceTexture): Boolean {
        controller.stop()
        return presentation?.onDestroyed(texture) ?: display.destroyed(texture)
    }

    /** A frame reached the view; [capture] reads it back only when the observer needs it. */
    public fun frameShown(texture: SurfaceTexture, capture: () -> PlaybackCapture?) {
        presentation?.onFrame(texture, texture.timestamp, capture)
    }

    /** Start [request], or stop the session that is running. */
    public fun toggle(request: PlaybackRequest) {
        val seat = presentation
        controller.toggle { reporter ->
            val playback =
                PlaybackSession(
                    request.versions,
                    request.start,
                    if (request.stats) STATS_EVERY else null,
                    onEvent = { event -> reporter.event(event) },
                ) { line -> reporter.report(line) }
            val attempt = seat?.attempt(
                request.objectBudget,
                PresentationSinks({ line -> reporter.report(line) }, { tag, line -> Log.i(tag, line) }),
            )
            val notice = { line: String -> reporter.report(line) }
            val host = if (attempt == null) AndroidPlaybackHost({ display.acquire() }, notice = notice)
            else AndroidPlaybackHost({ display.acquire() }, attempt::onDisplayOwned, attempt::displayReleased, notice)
            playback.watchRendered(request.url, request.namespace, request.objectBudget, host, attempt)
        }
    }

    /** Stop the running session, if any. */
    public fun stop() {
        controller.stop()
    }

    /** Diagnostic only: answer the harness's one stats request on the session scope. */
    public fun requestDiagnostics() {
        PlaybackDiagnostics.request(scope)
    }

    /** The owner is going away: stop, and end every background job. */
    public fun destroy() {
        controller.destroy()
        scope.cancel()
    }

    private companion object {
        val STATS_EVERY = 1.seconds
    }
}
