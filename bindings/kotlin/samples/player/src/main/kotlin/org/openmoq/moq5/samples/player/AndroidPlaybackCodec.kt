package org.openmoq.moq5.samples.player

import android.media.MediaCodec
import android.media.MediaFormat
import android.os.Build
import android.view.Surface
import java.nio.ByteBuffer

/**
 * Android synchronous decoder calls, confined by PlaybackCodec's owner. The
 * supplied Surface is borrowed, never released here: MediaSync's input Surface
 * with audio, the display Surface itself on the video-only path. No
 * asynchronous codec mode is configured; every dequeue uses a zero timeout.
 */
internal class AndroidPlaybackCodec private constructor(
    private val codec: MediaCodec,
    private val audio: Boolean,
    private val owner: PlaybackResourceOwner<MediaCodec>,
) : PlaybackCodecBackend {
    private val info = MediaCodec.BufferInfo()

    override fun inputIndex(): Int = codec.dequeueInputBuffer(0L)

    override fun inputBuffer(index: Int): ByteBuffer =
        checkNotNull(codec.getInputBuffer(index)) { "decoder returned no input buffer" }

    override fun queueInput(index: Int, size: Int, presentationUs: Long, end: Boolean) {
        PlaybackTrace.around("queueInputBuffer audio=$audio index=$index size=$size pts=$presentationUs end=$end") {
            codec.queueInputBuffer(index, 0, size, presentationUs, if (end) MediaCodec.BUFFER_FLAG_END_OF_STREAM else 0)
        }
    }

    override fun output(): PlaybackCodecEvent {
        val index = codec.dequeueOutputBuffer(info, 0L)
        if (index == LEGACY_OUTPUT_BUFFERS_CHANGED) return PlaybackCodecEvent.Idle
        return when (index) {
            MediaCodec.INFO_TRY_AGAIN_LATER -> PlaybackCodecEvent.Idle
            MediaCodec.INFO_OUTPUT_FORMAT_CHANGED -> {
                PlaybackTrace.mark { "output format audio=$audio" }
                PlaybackCodecEvent.Format(outputFacts(codec.outputFormat))
            }
            else -> {
                check(index >= 0) { "unrecognized decoder dequeue result $index" }
                PlaybackTrace.mark { "output buffer audio=$audio index=$index size=${info.size} pts=${info.presentationTimeUs} flags=${info.flags}" }
                PlaybackCodecEvent.Buffer(
                    index = index,
                    bytes = if (audio) codec.getOutputBuffer(index) else null,
                    offset = info.offset,
                    size = info.size,
                    presentationUs = info.presentationTimeUs,
                    end = info.flags and MediaCodec.BUFFER_FLAG_END_OF_STREAM != 0,
                    config = info.flags and MediaCodec.BUFFER_FLAG_CODEC_CONFIG != 0,
                )
            }
        }
    }

    override fun releaseOutput(index: Int, renderTimeNs: Long?) {
        PlaybackTrace.around("releaseOutputBuffer audio=$audio index=$index render=$renderTimeNs") {
            if (renderTimeNs == null) codec.releaseOutputBuffer(index, false)
            else codec.releaseOutputBuffer(index, renderTimeNs)
        }
    }

    override fun stop() = codec.stop()
    override fun release() = owner.close()

    private fun outputFacts(format: MediaFormat): PlaybackOutputFormat =
        PlaybackOutputFormat(
            mime = checkNotNull(format.getString(MediaFormat.KEY_MIME)) { "decoder output has no MIME type" },
            sampleRate = format.optionalInt(MediaFormat.KEY_SAMPLE_RATE),
            channels = format.optionalInt(MediaFormat.KEY_CHANNEL_COUNT),
            channelMask = format.optionalInt(MediaFormat.KEY_CHANNEL_MASK),
            pcmEncoding = if (Build.VERSION.SDK_INT >= 24) format.optionalInt(MediaFormat.KEY_PCM_ENCODING) else null,
            width = format.optionalInt(MediaFormat.KEY_WIDTH),
            height = format.optionalInt(MediaFormat.KEY_HEIGHT),
        )

    private fun MediaFormat.optionalInt(key: String): Int? = if (containsKey(key)) getInteger(key) else null

    companion object {
        // The deprecated -3 notification requires no action with per-index
        // getOutputBuffer; keep it out of the @IntDef switch and use no
        // deprecated buffer-array API.
        private const val LEGACY_OUTPUT_BUFFERS_CHANGED = -3

        fun video(format: FormatOutcome.Video, surface: Surface, maxUnitBytes: Int, owner: PlaybackResourceOwner<MediaCodec>): PlaybackCodec {
            require(maxUnitBytes > 0)
            val configured = MediaFormat.createVideoFormat(format.mime, format.width, format.height)
            return create(format.mime, configured, format.csd, surface, false, maxUnitBytes, owner)
        }

        fun audio(format: FormatOutcome.Audio, maxUnitBytes: Int, owner: PlaybackResourceOwner<MediaCodec>): PlaybackCodec {
            require(maxUnitBytes > 0)
            val configured = MediaFormat.createAudioFormat(format.mime, format.aac.samplingFrequency, format.aac.channelCount)
            return create(format.mime, configured, format.csd, null, true, maxUnitBytes, owner)
        }

        private fun create(
            mime: String,
            configured: MediaFormat,
            csd: List<ByteArray>,
            surface: Surface?,
            audio: Boolean,
            maxUnitBytes: Int,
            owner: PlaybackResourceOwner<MediaCodec>,
        ): PlaybackCodec {
            configured.setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, maxUnitBytes)
            csd.forEachIndexed { index, bytes -> configured.setByteBuffer("csd-$index", ByteBuffer.wrap(bytes.copyOf())) }
            val codec = owner.prepare({ PlaybackTrace.around("createDecoderByType $mime") { MediaCodec.createDecoderByType(mime) } }) { codec ->
                PlaybackTrace.mark { "decoder ${codec.name} for $mime" }
                PlaybackTrace.around("configure ${codec.name}") { codec.configure(configured, surface, null, 0) }
                PlaybackTrace.around("start ${codec.name}") { codec.start() }
            }
            return PlaybackCodec(AndroidPlaybackCodec(codec, audio, owner), audio, maxUnitBytes)
        }
    }
}
