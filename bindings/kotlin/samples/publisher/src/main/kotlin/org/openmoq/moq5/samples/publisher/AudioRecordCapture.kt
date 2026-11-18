package org.openmoq.moq5.samples.publisher

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.AudioTimestamp
import android.media.MediaRecorder
import android.os.Build
import java.nio.ByteBuffer

/**
 * The platform [MicrophonePort]: `AudioRecord` from the microphone, its
 * position notifications posted to the owned worker's [HandlerDispatcher].
 * Every decision -- the codec lease, the transactional open, admission, the
 * pump, the watchdog -- is [AudioPart]'s; this file holds only the platform
 * calls.
 *
 * `AudioRecord.Builder.build` requires the RECORD_AUDIO permission. The
 * session's permission flow refuses a start without it; the check here is the
 * platform's own, made in the same call so a revoked grant fails where it
 * stands.
 */
internal class AudioRecordPort(private val context: Context, private val dispatcher: HandlerDispatcher) : MicrophonePort {
    override fun build(configuration: AudioEncoderConfiguration): MicrophoneLink {
        if (context.checkSelfPermission(Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED) {
            throw SecurityException("the RECORD_AUDIO permission is not granted")
        }
        val mask = if (configuration.channelCount == 1) AudioFormat.CHANNEL_IN_MONO else AudioFormat.CHANNEL_IN_STEREO
        val minimum = AudioRecord.getMinBufferSize(configuration.sampleRate, mask, AudioFormat.ENCODING_PCM_16BIT)
        if (minimum <= 0) throw IllegalStateException("the microphone reports no buffer size for this format: $minimum")
        val record =
            AudioRecord.Builder()
                .setAudioSource(MediaRecorder.AudioSource.MIC)
                .setAudioFormat(
                    AudioFormat.Builder()
                        .setSampleRate(configuration.sampleRate)
                        .setChannelMask(mask)
                        .setEncoding(AudioFormat.ENCODING_PCM_16BIT)
                        .build(),
                )
                .setBufferSizeInBytes(maxOf(minimum, 2 * configuration.maxInputBytes))
                .build()
        return RecordLink(record, dispatcher)
    }
}

private class RecordLink(private val record: AudioRecord, private val dispatcher: HandlerDispatcher) : MicrophoneLink {
    override val identity: Any get() = record

    override val initialized: Boolean get() = record.state == AudioRecord.STATE_INITIALIZED

    override val format: MicrophoneFormat
        get() = MicrophoneFormat(record.sampleRate, record.channelCount, record.audioFormat == AudioFormat.ENCODING_PCM_16BIT)

    override val bufferSizeInFrames: Int get() = record.bufferSizeInFrames

    override fun listen(periodFrames: Int, wake: () -> Unit): Int {
        record.setRecordPositionUpdateListener(
            object : AudioRecord.OnRecordPositionUpdateListener {
                override fun onMarkerReached(recorder: AudioRecord) = Unit

                override fun onPeriodicNotification(recorder: AudioRecord) = wake()
            },
            dispatcher.handler(),
        )
        return record.setPositionNotificationPeriod(periodFrames)
    }

    override fun start() = record.startRecording()

    override val recording: Boolean get() = record.recordingState == AudioRecord.RECORDSTATE_RECORDING

    override fun read(into: ByteBuffer, bytes: Int): Int = record.read(into, bytes, AudioRecord.READ_NON_BLOCKING)

    /** `getTimestamp` exists from API 24; below it there is none, and the session refuses audio before this is reached. */
    override fun timestamp(): AudioTimestampFacts? {
        if (Build.VERSION.SDK_INT < CaptureClockPolicy.AUDIO_TIMESTAMP_SDK) return null
        val stamp = AudioTimestamp()
        if (record.getTimestamp(stamp, AudioTimestamp.TIMEBASE_BOOTTIME) != AudioRecord.SUCCESS) return null
        return AudioTimestampFacts(stamp.framePosition, stamp.nanoTime, PlatformTimebase.ElapsedRealtime)
    }

    override fun detach() = record.setRecordPositionUpdateListener(null)

    override fun stop() = record.stop()

    override fun release() = record.release()
}
