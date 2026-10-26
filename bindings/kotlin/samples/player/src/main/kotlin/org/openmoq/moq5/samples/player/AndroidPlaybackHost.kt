package org.openmoq.moq5.samples.player

import android.os.Handler
import android.os.HandlerThread
import android.view.Surface
import java.util.concurrent.RejectedExecutionException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

/** Owns only its newly constructed thread; never wraps or quits the UI looper. */
internal class AndroidPlaybackWorker : PlaybackRenderWorker {
    private val thread = HandlerThread("moq5-player-render")
    @Volatile private var handler: Handler? = null
    private var attempted = false
    override val dispatcher: CoroutineDispatcher = object : CoroutineDispatcher() {
        override fun dispatch(context: CoroutineContext, block: Runnable) {
            val target = handler ?: throw RejectedExecutionException("the player renderer worker is unavailable")
            if (!target.post(block)) throw RejectedExecutionException("the player renderer worker refused work")
        }
    }

    override fun start() {
        check(!attempted) { "player worker start is one-shot" }
        attempted = true
        thread.start()
        handler = Handler(checkNotNull(thread.looper) { "the player worker failed to establish its looper" })
    }

    override suspend fun stopAndJoin(): Boolean = withContext(Dispatchers.IO) {
        check(Thread.currentThread() !== thread) { "a renderer cannot join itself" }
        if (thread.isAlive) thread.quitSafely()
        // A bounded failed join retains ownership; it is never release proof.
        thread.join(JOIN_LIMIT_MS)
        !thread.isAlive
    }

    private companion object { const val JOIN_LIMIT_MS = 2_000L }
}

/**
 * Display acquisition supplies an owned lease; the renderer only borrows its
 * Surface. The display owner must outlive SurfaceTexture destruction until
 * that lease returns. The Activity must not fabricate a lease over a surface
 * which the framework may destroy underneath the asynchronous renderer.
 */
internal class AndroidPlaybackHost(
    private val acquireDisplay: () -> PlaybackDisplay<Surface>,
    private val onDisplayOwned: (PlaybackDisplay<Surface>) -> Unit = {},
    private val onReleased: () -> Unit = {},
) : PlaybackRenderHost {
    override suspend fun run(
        video: FormatOutcome.Video, audio: FormatOutcome.Audio?,
        consume: suspend (PlaybackRenderPort, PlaybackFailure) -> PlaybackReadResult,
    ): PlaybackReadResult = runOwnedPlayback(
        PROCESS_GATE, ::AndroidPlaybackWorker, acquireDisplay,
        { surface, failed -> AndroidPlaybackRender(surface, video, audio, failed) }, consume, onDisplayOwned, onReleased,
    )

    private companion object {
        // A failed native release must survive Activity recreation. Retaining
        // one uncertain attempt also blocks all later attempts in this process.
        val PROCESS_GATE = PlaybackRuntimeGate()
    }
}
