package org.openmoq.moq5.samples.player

import android.media.AudioAttributes
import android.media.AudioFormat
import android.media.AudioTrack
import android.os.Build

/**
 * An actual streaming AudioTrack for MediaSync, configured from decoder output.
 * The renderer owns the returned track and releases it only after MediaSync
 * has stopped using it. MediaSync, not this factory, controls playback speed
 * and writes PCM. The output Surface and codec lifetimes are separate.
 */
internal object AndroidPlaybackAudio : PlaybackAudioFactory<AudioTrack> {
    fun open(format: PlaybackOutputFormat, owner: PlaybackResourceOwner<AudioTrack>): AudioTrack = createPlaybackAudio(format, Build.VERSION.SDK_INT, this, owner)

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
