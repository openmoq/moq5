package org.openmoq.moq5.samples.showcase

import android.app.Application
import android.graphics.SurfaceTexture
import android.os.Handler
import android.os.Looper
import android.util.Log
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.viewModelScope
import java.io.File
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import org.openmoq.moq5.samples.playback.AndroidPlayback
import org.openmoq.moq5.samples.playback.PlaybackRequest
import org.openmoq.moq5.samples.playback.UiDispatch

/**
 * The Android side of [ShowcaseCore]: it owns the engine's screen-level
 * [AndroidPlayback], so a session outlives a configuration change, audio
 * focus ([AudioPolicy]) and the saved state. The activity forwards its video surface and lifecycle here.
 * Main thread only.
 */
class ShowcaseViewModel(application: Application) : AndroidViewModel(application) {
    private val store = ShowcaseStore(AtomicSavedFile(File(application.filesDir, "showcase.txt"))) { message, failure -> Log.w(TAG, message, failure) }

    /** Saving is serial and off the main thread: a later save can never be overtaken by an earlier one. */
    private val io = Dispatchers.IO.limitedParallelism(1)
    private val main = Handler(Looper.getMainLooper())
    private val playback: AndroidPlayback = AndroidPlayback(UiDispatch { action -> main.post(action) }) { update -> core.onUpdate(update) }

    /** Focus for the app's sessions; the core asks it before each start, and its interruptions stop the session. */
    private val audio: AudioPolicy = AudioPolicy(AndroidAudioFocus(application)) { reason -> core.interrupt(reason) }
    private val core: ShowcaseCore =
        ShowcaseCore(
            engine =
                object : PlaybackPort {
                    override fun start(request: PlaybackRequest) = playback.toggle(request)

                    override fun stop() = playback.stop()
                },
            focus = audio,
            post = { action -> main.post(action) },
            persist = { saved -> viewModelScope.launch(io) { store.save(saved) } },
            saved = store.load(),
        )

    /** What the UI draws. */
    val ui: StateFlow<ShowcaseUi> = core.ui

    fun editUrl(url: String) = core.editUrl(url)

    fun editNamespace(namespace: String) = core.editNamespace(namespace)

    fun changeSettings(settings: ShowcaseSettings) = core.changeSettings(settings)

    fun watch(target: WatchTarget) = core.watch(target)

    fun open(link: WatchLink) = core.open(link)

    fun play() = core.play()

    fun stop() = core.stop()

    fun leave() = core.leave()

    fun removeRecent(target: WatchTarget) = core.removeRecent(target)

    fun clearNotice() = core.clearNotice()

    /** The player's TextureView has its texture. */
    fun surfaceAvailable(texture: SurfaceTexture) {
        playback.surfaceAvailable(texture)
        core.surface(ready = true)
    }

    /** The player's texture is going; the engine stops any session and says who releases the texture. */
    fun surfaceDestroyed(texture: SurfaceTexture): Boolean {
        core.surface(ready = false)
        return playback.surfaceDestroyed(texture)
    }

    override fun onCleared() {
        core.close()
        playback.destroy()
        main.removeCallbacksAndMessages(null)
    }

    private companion object {
        const val TAG = "MoQ5Showcase"
    }
}
