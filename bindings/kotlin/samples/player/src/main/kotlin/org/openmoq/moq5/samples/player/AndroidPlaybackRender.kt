package org.openmoq.moq5.samples.player

import android.media.AudioTrack
import android.media.MediaCodec
import android.media.MediaSync
import android.os.Build
import android.view.Surface
import java.nio.ByteBuffer

/**
 * Actual Android assembly over the host-tested decoder/sync/render owners.
 * Construct on the renderer's serial looper; construction acquires nothing.
 * The caller owns this object BEFORE [start], so even partial startup failure
 * can run [close] and retain it when native disposal cannot be proved.
 *
 * The output Surface is borrowed and never released here. The caller keeps it
 * valid until close has released every renderer into it: Sync with audio, the
 * video decoder on the video-only path, where the decoder renders to it
 * directly and no MediaSync or proxy input Surface exists. Never call from a
 * MediaSync callback reentrantly. [wakeFailure] is nonthrowing and must wake/cancel a suspended
 * receiver operation; lifecycle observation must not depend on polling.
 *
 * Close is an abort/stop, not a claim that all sound was heard. Decoder EOS,
 * buffer consumption and actual presentation completion remain distinct.
 */
internal class AndroidPlaybackRender(
    private val output: Surface,
    private val videoFormat: FormatOutcome.Video,
    private val audioFormat: FormatOutcome.Audio?,
    private val wakeFailure: (Throwable) -> Unit,
) : ManagedPlaybackRender {
    private val owner = Thread.currentThread()
    private val syncOwner = PlaybackResourceOwner<MediaSync> { it.release() }
    private val videoOwner = PlaybackResourceOwner<MediaCodec> { it.release() }
    private val audioOwner = PlaybackResourceOwner<MediaCodec> { it.release() }
    private val trackOwner = PlaybackResourceOwner<AudioTrack> { it.release() }
    private val surfaceOwner = PlaybackResourceOwner<Surface> { it.release() }
    private var platform: AndroidPlaybackSync? = null
    private var sync: PlaybackSync? = null
    private var video: PlaybackCodec? = null
    private var audio: PlaybackCodec? = null
    private var track: AudioTrack? = null
    private var pcm: PlaybackPcm? = null
    private var loop: PlaybackRenderPump? = null
    private var attemptedStart = false
    private var closeResult: PlaybackRenderClosed? = null
    private var callbackFailure: Throwable? = null
    private var eosSnapshotTaken = false

    private fun checkOwner() { check(Thread.currentThread() === owner) { "renderer used off its owner thread" } }

    override fun start() {
        checkOwner()
        check(!attemptedStart && closeResult == null) { "renderer start is one-shot" }
        attemptedStart = true
        val target = playbackVideoTarget(output, audioFormat != null, ::startSync)
        if (audioFormat == null) PlaybackTrace.mark { "video direct to the display surface" }
        val v = AndroidPlaybackCodec.video(videoFormat, target, ACCESS_UNIT_LIMIT, videoOwner)
        video = v
        val a = audioFormat?.let { AndroidPlaybackCodec.audio(it, ACCESS_UNIT_LIMIT, audioOwner) }
        audio = a
        val paced = if (a == null) PlaybackVideoPacing(System::nanoTime) else null
        loop = PlaybackRenderPump(v, a, sync, ::configureAudio, pacing = paced)
    }

    /** The A/V path: MediaSync over the display Surface; returns its proxy input Surface. */
    private fun startSync(display: Surface): Surface {
        val native = PlaybackTrace.around("MediaSync create") { AndroidPlaybackSync.create(display, object : PlaybackSyncCallbacks {
            override fun onAudioConsumed(token: Int, buffer: ByteBuffer) {
                if (closeResult != null) return
                val current = sync
                if (current == null) failed(IllegalStateException("sync callback before its owner was bound"))
                else current.onAudioConsumed(token, buffer)
            }
            override fun onFailure(cause: Throwable) {
                if (closeResult != null) return
                val current = sync
                if (current == null) failed(cause) else current.onFailure(cause)
            }
        }, syncOwner, surfaceOwner) }
        platform = native
        sync = PlaybackSync(object : PlaybackSyncBackend {
            override fun queueAudio(bytes: ByteBuffer, token: Int, presentationUs: Long) {
                val layout = checkNotNull(pcm) { "PCM submitted before AudioTrack initialization" }
                require(bytes.remaining() % layout.frameBytes == 0) { "decoder output ends inside a PCM frame" }
                native.queueAudio(bytes, token, presentationUs)
            }
            override fun flush() = native.flush()
            override fun release() = native.release()
        }, 32, PCM_BYTE_LIMIT, { checkNotNull(audio).release(it) }, ::failed)
        return native.inputSurface
    }

    private fun configureAudio(format: PlaybackOutputFormat) {
        check(track == null) { "AudioTrack is initialized once" }
        val layout = PlaybackPcm.from(format, Build.VERSION.SDK_INT)
        val opened = AndroidPlaybackAudio.open(format, trackOwner)
        // Publish ownership before the platform can partially adopt the track.
        track = opened
        pcm = layout
        PlaybackTrace.mark {
            "pcm-format rate=${format.sampleRate} channels=${format.channels} mask=${format.channelMask} " +
                "encoding=${format.pcmEncoding} frameBytes=${layout.frameBytes}"
        }
        checkNotNull(platform).configureAudio(opened)
        checkNotNull(platform).play()
    }

    private fun failed(cause: Throwable) {
        if (closeResult != null || callbackFailure != null) return
        callbackFailure = cause
        wakeFailure(cause)
    }

    private fun running(): PlaybackRenderPump {
        checkOwner()
        check(closeResult == null) { "renderer has closed" }
        callbackFailure?.let { throw it }
        return checkNotNull(loop) { "renderer did not start" }
    }

    override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long): PlaybackOffer = running().offer(lane, unit, logicalUs)
    override fun pump(): Boolean = running().pump()
    override fun endInputs() = running().endInputs()
    override val decoderOutputsEnded: Boolean get() {
        val ended = running().decoderOutputsEnded
        if (ended && !eosSnapshotTaken) {
            eosSnapshotTaken = true
            audioSinkSnapshot("decoder-eos")
        }
        return ended
    }

    /**
     * Diagnostic-only, on the owner thread: the AudioTrack's playback head (frames the sink has
     * consumed) and play state, plus this renderer's PCM accounting. A head position is not acoustic
     * proof, and a snapshot taken at decoder EOS may precede the end of playback.
     */
    private fun audioSinkSnapshot(at: String) {
        val t = track ?: return
        val s = sync
        PlaybackTrace.mark {
            "audio-sink at=$at head=${t.playbackHeadPosition} state=${t.playState} " +
                "submitted=${s?.submitted} submittedBytes=${s?.submittedBytes} returned=${s?.returned} " +
                "returnedBytes=${s?.returnedBytes}"
        }
    }
    /** Readable after a pump or callback failure, while this renderer is still owned and open. */
    override val videoOutputsReleased: Int get() {
        checkOwner()
        check(closeResult == null) { "renderer has closed" }
        return loop?.videoReleased ?: 0
    }

    /**
     * Always call after a start attempt, including one that threw. Retain THIS
     * object if returned resources remain; do not retry uncertain operations
     * or free the borrowed output Surface under a retained Sync.
     */
    override fun close(): PlaybackRenderClosed {
        checkOwner()
        closeResult?.let { return it }
        runCatching { audioSinkSnapshot("close") }
        loop?.stop()
        val manager = sync
        // The manager is assigned before decoder creation; without it, no
        // queueAudio was possible. The raw owner includes failed construction:
        // its close replays the cached result, never retries an uncertain release.
        val closeSync: (() -> PlaybackSyncClosed)? = when {
            manager != null -> ({ manager.close() })
            syncOwner.acquired -> ({
                val raised = runCatching { syncOwner.close() }.exceptionOrNull()
                PlaybackSyncClosed(raised == null, raised == null, raised)
            })
            else -> null
        }
        return disposePlaybackRender(
            sync = closeSync,
            video = if (videoOwner.acquired) ({ video?.close() ?: videoOwner.close() }) else null,
            audio = if (audioOwner.acquired) ({ audio?.close() ?: audioOwner.close() }) else null,
            track = if (trackOwner.acquired) ({ trackOwner.close() }) else null,
            surface = if (surfaceOwner.acquired) ({ surfaceOwner.close() }) else null,
        ).also { closeResult = it }
    }

    private companion object {
        const val ACCESS_UNIT_LIMIT = 8 * 1024 * 1024
        const val PCM_BYTE_LIMIT = 4 * 1024 * 1024
    }
}
