package org.openmoq.moq5.samples.player

import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioTimestamp
import android.media.AudioTrack
import android.os.Build
import java.nio.ByteBuffer

/**
 * An actual streaming AudioTrack, configured from decoder output. The
 * renderer owns the returned track and releases it at close; the PCM written
 * to it is copied, so no decoder buffer outlives its write.
 */
internal object AndroidPlaybackAudio : PlaybackAudioFactory<AudioTrack> {
    fun open(format: PlaybackOutputFormat, owner: PlaybackResourceOwner<AudioTrack>): AudioTrack = createPlaybackAudio(format, Build.VERSION.SDK_INT, this, owner)

    /** The renderer's nonblocking view of [track]; it borrows the track and never releases it. */
    fun sink(track: AudioTrack): PlaybackAudioSink = object : PlaybackAudioSink {
        private val reading = AudioTimestamp()

        override fun write(bytes: ByteBuffer): Int = track.write(bytes, bytes.remaining(), AudioTrack.WRITE_NON_BLOCKING)

        override fun head(): Int = track.playbackHeadPosition

        override fun timestamp(): PlaybackAudioTimestamp? =
            if (track.getTimestamp(reading)) PlaybackAudioTimestamp(reading.framePosition, reading.nanoTime) else null

        override fun play() = track.play()
    }

    override fun minimumBytes(pcm: PlaybackPcm): Int =
        AudioTrack.getMinBufferSize(pcm.sampleRate, pcm.channelMask, pcm.encoding)

    override fun create(pcm: PlaybackPcm, bufferBytes: Int): AudioTrack =
        AudioTrack.Builder()
            .setAudioAttributes(
                AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_MEDIA)
                    .setContentType(AudioAttributes.CONTENT_TYPE_MOVIE)
                    .build(),
            )
            .setAudioFormat(
                AudioFormat.Builder()
                    .setSampleRate(pcm.sampleRate)
                    .setChannelMask(pcm.channelMask)
                    .setEncoding(pcm.encoding)
                    .build(),
            )
            .setTransferMode(AudioTrack.MODE_STREAM)
            .setBufferSizeInBytes(bufferBytes)
            .build()

    override fun initialized(track: AudioTrack): Boolean = track.state == AudioTrack.STATE_INITIALIZED

    override fun release(track: AudioTrack) = track.release()
}
