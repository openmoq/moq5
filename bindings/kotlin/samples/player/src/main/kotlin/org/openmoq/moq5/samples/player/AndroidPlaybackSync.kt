package org.openmoq.moq5.samples.player

import android.media.AudioTrack
import android.media.MediaSync
import android.media.PlaybackParams
import android.os.Handler
import android.os.Looper
import android.view.Surface
import java.nio.ByteBuffer

/** Actual platform error values, retained without guessing their cause. */
internal class PlaybackPlatformSyncException(val what: Int, val extra: Int) :
    IllegalStateException("MediaSync failed: what=$what extra=$extra")

/**
 * Platform MediaSync on the renderer's looper. The callback Handler is unique
 * to this sync: flush may remove every message on it. Output Surface and
 * AudioTrack are BORROWED. The newly created [inputSurface] transfers to the
 * renderer, which releases it after the video codec is finished with it.
 */
internal class AndroidPlaybackSync private constructor(
    private val sync: MediaSync,
    private val owner: Thread,
    val inputSurface: Surface,
    private val nativeOwner: PlaybackResourceOwner<MediaSync>,
) : PlaybackSyncBackend {
    private fun ownerOnly() { check(Thread.currentThread() === owner) { "MediaSync used outside its renderer owner" } }

    /** Initialization only, before queueing/rendering/playback. */
    fun configureAudio(track: AudioTrack) {
        ownerOnly()
        sync.setAudioTrack(track)
    }

    fun play() {
        ownerOnly()
        sync.setPlaybackParams(PlaybackParams().allowDefaults().setSpeed(1f))
    }

    override fun queueAudio(bytes: ByteBuffer, token: Int, presentationUs: Long) {
        ownerOnly()
        sync.queueAudio(bytes, token, presentationUs)
    }

    override fun flush() { ownerOnly(); sync.flush() }
    override fun release() { ownerOnly(); nativeOwner.close() }

    companion object {
        fun create(
            surface: Surface,
            callbacks: PlaybackSyncCallbacks,
            owner: PlaybackResourceOwner<MediaSync>,
            inputOwner: PlaybackResourceOwner<Surface>,
        ): AndroidPlaybackSync {
            val looper = checkNotNull(Looper.myLooper()) { "the renderer requires an owner looper" }
            val handler = Handler(looper)
            var input: Surface? = null
            val sync = owner.prepare({ MediaSync() }) { sync ->
                sync.setCallback(object : MediaSync.Callback() {
                    override fun onAudioBufferConsumed(source: MediaSync, audioBuffer: ByteBuffer, bufferId: Int) {
                        if (source !== sync) callbacks.onFailure(IllegalStateException("foreign MediaSync audio callback"))
                        else callbacks.onAudioConsumed(bufferId, audioBuffer)
                    }
                }, handler)
                sync.setOnErrorListener({ source, what, extra ->
                    if (source !== sync) callbacks.onFailure(IllegalStateException("foreign MediaSync error callback"))
                    else callbacks.onFailure(PlaybackPlatformSyncException(what, extra))
                }, handler)
                sync.setSurface(surface)
                input = inputOwner.prepare({ sync.createInputSurface() }) {}
            }
            return AndroidPlaybackSync(sync, Thread.currentThread(), checkNotNull(input), owner)
        }
    }
}
