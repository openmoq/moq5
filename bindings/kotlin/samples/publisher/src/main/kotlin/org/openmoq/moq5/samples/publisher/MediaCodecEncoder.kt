package org.openmoq.moq5.samples.publisher

import android.media.MediaCodec
import android.media.MediaCodecInfo
import android.media.MediaFormat

/**
 * The platform [EncoderBackend]: `MediaCodec` in asynchronous mode, its
 * callbacks posted to the owned worker's [HandlerDispatcher].
 *
 * The allocate/check/transfer-or-release control flow is [AllocatingBackend]'s,
 * the same one the host rows drive; this supplies only the platform calls. The
 * encoder is created by MIME type and its reported capabilities are checked by
 * [EncoderCapabilityCheck] before anything is configured.
 */
internal class MediaCodecBackend(dispatcher: HandlerDispatcher) : EncoderBackend by AllocatingBackend(MediaCodecAllocation(dispatcher))

/**
 * The class a camera stream is asked about when it will draw into an encoder's
 * input surface (`StreamConfigurationMap.getOutputSizes(Class)`). Named here so
 * the platform encoder stays confined to this file.
 */
internal val encoderSurfaceClass: Class<*> = MediaCodec::class.java

/** The four `MediaCodec` calls [AllocatingBackend] sequences. */
private class MediaCodecAllocation(private val dispatcher: HandlerDispatcher) : EncoderAllocationPort<MediaCodec> {
    override fun allocate(configuration: EncoderConfiguration): MediaCodec = MediaCodec.createEncoderByType(configuration.mime)

    override fun capabilities(codec: MediaCodec, configuration: EncoderConfiguration): EncoderCapabilities =
        PlatformCapabilities(codec.codecInfo.getCapabilitiesForType(configuration.mime))

    override fun release(codec: MediaCodec) = codec.release()

    override fun device(codec: MediaCodec, configuration: EncoderConfiguration): EncoderDevice = MediaCodecDevice(codec, dispatcher)
}

/** The capabilities `MediaCodecInfo` reports for one MIME type, as [EncoderCapabilities]. */
private class PlatformCapabilities(private val capabilities: MediaCodecInfo.CodecCapabilities) : EncoderCapabilities {
    override fun sizeAndRateSupported(width: Int, height: Int, frameRate: Int): Boolean =
        capabilities.videoCapabilities?.areSizeAndRateSupported(width, height, frameRate.toDouble()) ?: false

    override fun bitrateSupported(bitrate: Int): Boolean =
        (capabilities.videoCapabilities?.bitrateRange ?: capabilities.audioCapabilities?.bitrateRange)?.contains(bitrate) ?: false

    override val surfaceInput: Boolean
        get() = capabilities.colorFormats.contains(MediaCodecInfo.CodecCapabilities.COLOR_FormatSurface)

    override fun sampleRateSupported(sampleRate: Int): Boolean = capabilities.audioCapabilities?.isSampleRateSupported(sampleRate) ?: false

    override val maxInputChannels: Int
        get() = capabilities.audioCapabilities?.maxInputChannelCount ?: 0

    override val aacLowComplexity: Boolean
        get() = capabilities.profileLevels.any { it.profile == MediaCodecInfo.CodecProfileLevel.AACObjectLC }
}

/** One created `MediaCodec`, driven only through [TrackEncoder]. */
private class MediaCodecDevice(private val codec: MediaCodec, private val dispatcher: HandlerDispatcher) : EncoderDevice {
    override val identity: Any get() = codec

    override fun setCallbacks(callbacks: EncoderCallbacks) = codec.setCallback(Relay(callbacks), dispatcher.handler())

    override fun configure(configuration: EncoderConfiguration) = codec.configure(format(configuration), null, null, MediaCodec.CONFIGURE_FLAG_ENCODE)

    override fun createInputSurface(): ResourceHandle {
        val surface = codec.createInputSurface()
        return object : ResourceHandle {
            override val identity: Any = surface

            override fun close() = surface.release()
        }
    }

    override fun start() = codec.start()

    override fun outputBuffer(index: Int) = codec.getOutputBuffer(index)

    override fun releaseOutputBuffer(index: Int) = codec.releaseOutputBuffer(index, false)

    override fun inputBuffer(index: Int) = codec.getInputBuffer(index)

    override fun queueInputBuffer(index: Int, size: Int, presentationTimeUs: Long) = codec.queueInputBuffer(index, 0, size, presentationTimeUs, 0)

    override fun stop() = codec.stop()

    override fun release() = codec.release()

    /** The requested settings, exactly, under the platform's own keys. */
    private fun format(configuration: EncoderConfiguration): MediaFormat =
        when (configuration) {
            is VideoEncoderConfiguration ->
                MediaFormat.createVideoFormat(configuration.mime, configuration.width, configuration.height).apply {
                    setInteger(MediaFormat.KEY_COLOR_FORMAT, MediaCodecInfo.CodecCapabilities.COLOR_FormatSurface)
                    setInteger(MediaFormat.KEY_BIT_RATE, configuration.bitrate)
                    setInteger(MediaFormat.KEY_FRAME_RATE, configuration.frameRate)
                    setInteger(MediaFormat.KEY_I_FRAME_INTERVAL, configuration.keyFrameIntervalSeconds)
                }
            is AudioEncoderConfiguration ->
                MediaFormat.createAudioFormat(configuration.mime, configuration.sampleRate, configuration.channelCount).apply {
                    setInteger(MediaFormat.KEY_AAC_PROFILE, MediaCodecInfo.CodecProfileLevel.AACObjectLC)
                    setInteger(MediaFormat.KEY_BIT_RATE, configuration.bitrate)
                    setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, configuration.maxInputBytes)
                }
        }
}

/**
 * The platform callback, taken apart on the owned worker's thread: the four
 * `BufferInfo` scalars are copied before anything else runs, since the object
 * is the platform's; the output format is read only through the holder, inside
 * its admission.
 */
private class Relay(private val to: EncoderCallbacks) : MediaCodec.Callback() {
    override fun onOutputBufferAvailable(codec: MediaCodec, index: Int, info: MediaCodec.BufferInfo) =
        to.onOutputAvailable(index, info.offset, info.size, info.presentationTimeUs, info.flags)

    override fun onOutputFormatChanged(codec: MediaCodec, format: MediaFormat) = to.onFormatChanged(MediaFormatFields(format))

    override fun onInputBufferAvailable(codec: MediaCodec, index: Int) = to.onInputAvailable(index)

    override fun onError(codec: MediaCodec, e: MediaCodec.CodecException) = to.onError(e.diagnosticInfo ?: "codec error")
}
