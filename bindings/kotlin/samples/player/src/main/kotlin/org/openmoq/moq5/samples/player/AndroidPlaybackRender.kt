package org.openmoq.moq5.samples.player

import android.media.AudioTrack
import android.media.MediaCodec
import android.os.Build
import android.view.Surface

/**
 * Actual Android assembly over the host-tested decoder, audio and render
 * owners. Construct on the renderer's serial looper; construction acquires
 * nothing. The caller owns this object BEFORE [start], so even partial
 * startup failure can run [close] and retain it when native disposal cannot
 * be proved.
 *
 * The output Surface is borrowed and never released here; the video decoder
 * renders to it directly. The caller keeps it valid until close has released
 * that decoder. With audio, PCM goes to an AudioTrack written without
 * blocking, and video follows what that track presents (see
 * [PlaybackAudioOutput]). No platform object calls back into this renderer,
 * and nothing here waits on the UI thread. [notice] receives status lines
 * for the user, such as audio falling back to video only, and must not throw.
 *
 * Close is an abort/stop, not a claim that all sound was heard. Decoder EOS,
 * PCM written and actual presentation completion remain distinct.
 */
internal class AndroidPlaybackRender(
    private val output: Surface,
    private val videoFormat: FormatOutcome.Video,
    private val audioFormat: FormatOutcome.Audio?,
    private val notice: (String) -> Unit,
) : ManagedPlaybackRender {
    private val owner = Thread.currentThread()
    private val videoOwner = PlaybackResourceOwner<MediaCodec> { it.release() }
    private val audioOwner = PlaybackResourceOwner<MediaCodec> { it.release() }
    private val trackOwner = PlaybackResourceOwner<AudioTrack> { it.release() }
    private var video: PlaybackCodec? = null
    private var audio: PlaybackCodec? = null
    private var track: AudioTrack? = null
    private var audioOut: PlaybackAudioOutput? = null
    private var loop: PlaybackRenderPump? = null
    private var attemptedStart = false
    private var closeResult: PlaybackRenderClosed? = null
    private var eosSnapshotTaken = false

    private fun checkOwner() { check(Thread.currentThread() === owner) { "renderer used off its owner thread" } }

    override fun start() {
        checkOwner()
        check(!attemptedStart && closeResult == null) { "renderer start is one-shot" }
        attemptedStart = true
        val v = AndroidPlaybackCodec.video(videoFormat, output, ACCESS_UNIT_LIMIT, videoOwner)
        video = v
        val a = audioFormat?.let { AndroidPlaybackCodec.audio(it, ACCESS_UNIT_LIMIT, audioOwner) }
        audio = a
        val out = a?.let { PlaybackAudioOutput(::openAudio, System::nanoTime, notice) }
        audioOut = out
        PlaybackTrace.mark { if (out == null) "video paced on the display clock" else "video follows the audio clock" }
        loop = PlaybackRenderPump(v, a, out, PlaybackVideoPacing(System::nanoTime, out))
    }

    /** From the audio decoder's first PCM format: one initialized track, started playing by the output. */
    private fun openAudio(format: PlaybackOutputFormat): PlaybackAudioOpened {
        check(track == null) { "AudioTrack is initialized once" }
        val layout = PlaybackPcm.from(format, Build.VERSION.SDK_INT)
        val opened = AndroidPlaybackAudio.open(format, trackOwner)
        track = opened
        PlaybackTrace.mark {
            "pcm-format rate=${format.sampleRate} channels=${format.channels} mask=${format.channelMask} " +
                "encoding=${format.pcmEncoding} frameBytes=${layout.frameBytes}"
        }
        return PlaybackAudioOpened(AndroidPlaybackAudio.sink(opened), layout.sampleRate, layout.frameBytes)
    }

    private fun running(): PlaybackRenderPump {
        checkOwner()
        check(closeResult == null) { "renderer has closed" }
        return checkNotNull(loop) { "renderer did not start" }
    }

    override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long, group: ULong?): PlaybackOffer =
        running().offer(lane, unit, logicalUs, group)
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
     * consumed) and play state, the audio clock's accounting and the late video drops. A head
     * position is not acoustic proof, and a snapshot taken at decoder EOS may precede the end of
     * playback.
     */
    private fun audioSinkSnapshot(at: String) {
        val dropped = loop?.videoDroppedLate
        val t = track
        val out = audioOut
        PlaybackTrace.mark {
            "audio-sink at=$at head=${t?.playbackHeadPosition} state=${t?.playState} " +
                "${out?.summary()} video-late-drops=$dropped"
        }
    }

    /** Readable after a pump failure, while this renderer is still owned and open. */
    override val videoOutputsReleased: Int get() {
        checkOwner()
        check(closeResult == null) { "renderer has closed" }
        return loop?.videoReleased ?: 0
    }

    /**
     * Always call after a start attempt, including one that threw. Retain THIS
     * object if returned resources remain; do not retry uncertain operations
     * or free the borrowed output Surface under a video decoder that may still
     * render into it.
     */
    override fun close(): PlaybackRenderClosed {
        checkOwner()
        closeResult?.let { return it }
        runCatching { audioSinkSnapshot("close") }
        loop?.stop()
        return disposePlaybackRender(
            video = if (videoOwner.acquired) ({ video?.close() ?: videoOwner.close() }) else null,
            audio = if (audioOwner.acquired) ({ audio?.close() ?: audioOwner.close() }) else null,
            track = if (trackOwner.acquired) ({ trackOwner.close() }) else null,
        ).also { closeResult = it }
    }

    private companion object {
        const val ACCESS_UNIT_LIMIT = 8 * 1024 * 1024
    }
}
