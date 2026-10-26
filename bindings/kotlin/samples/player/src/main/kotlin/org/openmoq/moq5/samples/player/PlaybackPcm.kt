package org.openmoq.moq5.samples.player

/**
 * Validated decoder OUTPUT, not the catalog's compressed audio description.
 * Numeric encoding/mask values are Android AudioFormat's public constants.
 * Device routing/support is still determined by AudioTrack, not this check.
 */
internal class PlaybackPcm private constructor(
    val sampleRate: Int,
    val channels: Int,
    val channelMask: Int,
    val encoding: Int,
    val frameBytes: Int,
) {
    /** Two platform minimum buffers, rounded up to a frame, within a byte cap. */
    fun bufferBytes(minimum: Int, maximum: Int): Int {
        require(minimum > 0) { "AudioTrack refused the decoded PCM format (minimum=$minimum)" }
        require(maximum > 0) { "the audio buffer byte limit must be positive" }
        val wanted = minimum.toLong() * 2
        val aligned = ((wanted + frameBytes - 1) / frameBytes) * frameBytes
        require(aligned <= maximum.toLong()) { "AudioTrack buffer exceeds the $maximum byte playback limit" }
        return aligned.toInt()
    }

    companion object {
        fun from(format: PlaybackOutputFormat, api: Int): PlaybackPcm {
            require(api >= 23) { "the player requires Android API 23 or newer" }
            require(format.mime == "audio/raw") { "decoder output is not PCM: ${format.mime}" }
            val rate = requireNotNull(format.sampleRate) { "decoder PCM has no sample rate" }
            val count = requireNotNull(format.channels) { "decoder PCM has no channel count" }
            require(rate > 0 && count > 0) { "decoder PCM rate and channel count must be positive" }
            val mask = format.channelMask ?: when (count) {
                1 -> 0x4 // CHANNEL_OUT_MONO
                2 -> 0xc // CHANNEL_OUT_STEREO
                else -> throw IllegalArgumentException("multichannel decoder PCM has no speaker-position mask")
            }
            // Only speaker-position bits, not DEFAULT, index or haptic masks.
            // The platform remains responsible for supported combinations.
            require(mask > 0 && mask and 0x0ffffffc == mask && Integer.bitCount(mask) == count) {
                "decoder PCM channel mask does not describe its $count channels"
            }
            // MediaFormat documents signed PCM16 when KEY_PCM_ENCODING is absent.
            val encoding = format.pcmEncoding ?: 2
            val sampleBytes = when (encoding) {
                2 -> 2 // PCM_16BIT
                3 -> 1 // PCM_8BIT
                4 -> 4 // PCM_FLOAT
                21, 22 -> {
                    require(api >= 31) { "decoded PCM encoding $encoding requires Android API 31" }
                    if (encoding == 21) 3 else 4 // PCM_24BIT_PACKED, PCM_32BIT
                }
                else -> throw IllegalArgumentException("unsupported decoded PCM encoding $encoding")
            }
            return PlaybackPcm(rate, count, mask, encoding, count * sampleBytes)
        }
    }
}

/** Narrow AudioTrack creation boundary; create owns any failure before return. */
internal interface PlaybackAudioFactory<T : Any> {
    fun minimumBytes(pcm: PlaybackPcm): Int
    fun create(pcm: PlaybackPcm, bufferBytes: Int): T
    fun initialized(track: T): Boolean
    fun release(track: T)
}

/**
 * Returns one initialized, unstarted streaming track owned by [owner].
 * No playback is started. On failure after allocation, release is attempted
 * once and its failure stays secondary, including Error and cancellation.
 */
internal fun <T : Any> createPlaybackAudio(
    format: PlaybackOutputFormat,
    api: Int,
    factory: PlaybackAudioFactory<T>,
    owner: PlaybackResourceOwner<T>,
    maxBufferBytes: Int = 4 * 1024 * 1024,
): T {
    require(maxBufferBytes > 0) { "the audio buffer byte limit must be positive" }
    val pcm = PlaybackPcm.from(format, api)
    val bytes = pcm.bufferBytes(factory.minimumBytes(pcm), maxBufferBytes)
    return owner.prepare({ factory.create(pcm, bytes) }) { track ->
        check(factory.initialized(track)) { "AudioTrack did not initialize" }
    }
}
