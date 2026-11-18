package org.openmoq.moq5.samples.publisher

/**
 * What this sample asks an encoder for, validated BEFORE any platform call.
 *
 * These are REQUESTED settings. What the encoder actually produced -- its
 * dimensions, rate, channels and codec-specific data -- arrives later through
 * `onOutputFormatChanged` and is validated by the format holders; nothing here
 * stands in for it. [maximumBitrate] is the track's DECLARED maximum, the
 * value the format holder is given alongside the observed format; the
 * requested [bitrate] must not exceed it, and no encoder mode here claims to
 * enforce it.
 */
internal sealed interface EncoderConfiguration {
    val kind: MediaKind
    val mime: String
    val bitrate: Int
    val maximumBitrate: ULong

    /** Whether the encoder takes its input from a surface it creates, rather than from queued PCM buffers. */
    val surfaceInput: Boolean
}

/** AVC from a surface the camera stream draws into. */
internal class VideoEncoderConfiguration private constructor(
    val width: Int,
    val height: Int,
    val frameRate: Int,
    override val bitrate: Int,
    override val maximumBitrate: ULong,
    /** Seconds between sync frames; every group starts on one. */
    val keyFrameIntervalSeconds: Int,
) : EncoderConfiguration {
    override val kind: MediaKind get() = MediaKind.Video
    override val mime: String get() = FormatKeys.VIDEO_AVC
    override val surfaceInput: Boolean get() = true

    companion object {
        /** The configuration, or the named reason these settings are refused. Nothing is created either way. */
        fun of(width: Int, height: Int, frameRate: Int, bitrate: Int, maximumBitrate: ULong, keyFrameIntervalSeconds: Int): EncoderPlan<VideoEncoderConfiguration> =
            when {
                width <= 0 || height <= 0 -> EncoderPlan.Refused("the video size ${width}x$height is not positive")
                // 4:2:0 chroma subsampling needs both dimensions even.
                width % 2 != 0 || height % 2 != 0 -> EncoderPlan.Refused("the video size ${width}x$height is not even in both dimensions")
                frameRate <= 0 -> EncoderPlan.Refused("the video frame rate $frameRate is not positive")
                keyFrameIntervalSeconds <= 0 -> EncoderPlan.Refused("the key-frame interval of $keyFrameIntervalSeconds s is not positive")
                else -> bitrateRefusal(bitrate, maximumBitrate)?.let { EncoderPlan.Refused("video: $it") }
                    ?: EncoderPlan.Planned(VideoEncoderConfiguration(width, height, frameRate, bitrate, maximumBitrate, keyFrameIntervalSeconds))
            }
    }
}

/**
 * AAC-LC from queued 16-bit PCM. One access unit is [FRAMES_PER_UNIT]
 * frames; one input buffer holds at most one unit, [maxInputBytes].
 */
internal class AudioEncoderConfiguration private constructor(
    val sampleRate: Int,
    val channelCount: Int,
    override val bitrate: Int,
    override val maximumBitrate: ULong,
) : EncoderConfiguration {
    override val kind: MediaKind get() = MediaKind.Audio
    override val mime: String get() = FormatKeys.AUDIO_AAC
    override val surfaceInput: Boolean get() = false

    /** The largest PCM input this configuration queues at once: one unit of 16-bit samples. */
    val maxInputBytes: Int get() = FRAMES_PER_UNIT * channelCount * BYTES_PER_SAMPLE

    /**
     * The audio key authority for exactly this PCM geometry: its rate and its
     * unit length, from [originUs]. A broadcast keyed any other way would
     * place its units on a second timeline.
     */
    fun keying(originUs: Long): UnitAudioKeying = UnitAudioKeying(sampleRate, FRAMES_PER_UNIT, originUs)

    /** Why [keying] does not key this PCM geometry -- its rate, then its unit length -- or null when it does. */
    fun keyingDisagreement(keying: AudioKeying): String? =
        when {
            keying.sampleRate != sampleRate ->
                "the audio keying's sample rate ${keying.sampleRate} is not the configured PCM rate $sampleRate"
            keying.framesPerUnit != FRAMES_PER_UNIT ->
                "the audio keying's unit of ${keying.framesPerUnit} frames is not the configured AAC-LC unit of $FRAMES_PER_UNIT"
            else -> null
        }

    companion object {
        /** The AAC-LC access unit this sample configures: frameLengthFlag 0. */
        const val FRAMES_PER_UNIT = 1024

        /** 16-bit PCM, the encoder's default input encoding. */
        const val BYTES_PER_SAMPLE = 2

        /** The sampling frequencies an AudioSpecificConfig can index (ISO/IEC 14496-3, Table 1.18). */
        private val AAC_RATES = setOf(96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000, 7_350)

        /** The configuration, or the named reason these settings are refused. Nothing is created either way. */
        fun of(sampleRate: Int, channelCount: Int, bitrate: Int, maximumBitrate: ULong): EncoderPlan<AudioEncoderConfiguration> =
            when {
                sampleRate !in AAC_RATES -> EncoderPlan.Refused("the audio sample rate $sampleRate Hz has no AAC sampling-frequency index")
                channelCount != 1 && channelCount != 2 -> EncoderPlan.Refused("the audio channel count $channelCount is neither mono nor stereo")
                else -> bitrateRefusal(bitrate, maximumBitrate)?.let { EncoderPlan.Refused("audio: $it") }
                    ?: EncoderPlan.Planned(AudioEncoderConfiguration(sampleRate, channelCount, bitrate, maximumBitrate))
            }
    }
}

/** Requested settings, planned or refused by name before any platform call. */
internal sealed interface EncoderPlan<out C : EncoderConfiguration> {
    class Planned<C : EncoderConfiguration>(val configuration: C) : EncoderPlan<C>

    data class Refused(val reason: String) : EncoderPlan<Nothing>
}

private fun bitrateRefusal(bitrate: Int, maximumBitrate: ULong): String? =
    when {
        bitrate <= 0 -> "the requested bitrate $bitrate is not positive"
        bitrate.toULong() > maximumBitrate -> "the requested bitrate $bitrate exceeds the declared maximum $maximumBitrate"
        else -> null
    }

/**
 * What one created encoder says it supports, as the platform's codec
 * capabilities report it. Only the facts this sample's two roles need.
 */
internal interface EncoderCapabilities {
    fun sizeAndRateSupported(width: Int, height: Int, frameRate: Int): Boolean

    fun bitrateSupported(bitrate: Int): Boolean

    /** Whether the encoder accepts input from a surface. */
    val surfaceInput: Boolean

    fun sampleRateSupported(sampleRate: Int): Boolean

    val maxInputChannels: Int

    /** Whether the encoder lists the AAC Low Complexity profile. */
    val aacLowComplexity: Boolean
}

/** Refuses a configuration the created encoder does not support, by name, before it is configured. */
internal object EncoderCapabilityCheck {
    fun refusal(configuration: EncoderConfiguration, capabilities: EncoderCapabilities): String? =
        when (configuration) {
            is VideoEncoderConfiguration ->
                when {
                    !capabilities.surfaceInput -> "the AVC encoder does not accept surface input"
                    !capabilities.sizeAndRateSupported(configuration.width, configuration.height, configuration.frameRate) ->
                        "the AVC encoder does not support ${configuration.width}x${configuration.height} at ${configuration.frameRate} fps"
                    !capabilities.bitrateSupported(configuration.bitrate) -> "the AVC encoder does not support ${configuration.bitrate} bit/s"
                    else -> null
                }
            is AudioEncoderConfiguration ->
                when {
                    !capabilities.aacLowComplexity -> "the AAC encoder does not list the Low Complexity profile"
                    !capabilities.sampleRateSupported(configuration.sampleRate) -> "the AAC encoder does not support ${configuration.sampleRate} Hz"
                    capabilities.maxInputChannels < configuration.channelCount ->
                        "the AAC encoder takes at most ${capabilities.maxInputChannels} channel(s), not ${configuration.channelCount}"
                    !capabilities.bitrateSupported(configuration.bitrate) -> "the AAC encoder does not support ${configuration.bitrate} bit/s"
                    else -> null
                }
        }
}
