package org.openmoq.moq5.samples.publisher

import android.content.Context
import android.hardware.camera2.CameraAccessException
import android.hardware.camera2.CameraManager
import android.util.Log
import org.openmoq.moq5.EndpointConfig
import org.openmoq.moq5.MediaSender
import org.openmoq.moq5.MoqEndpoint
import org.openmoq.moq5.SendTrack
import org.openmoq.moq5.SenderConfig
import org.openmoq.moq5.UnixMicros
import kotlin.time.Duration.Companion.seconds

/**
 * The live broadcast's platform: the back camera's clock, the running build's
 * audio clock, one calibration, and [PlatformCapture.of]'s capture kit. Each
 * fact is read through the accepted decision; nothing here decides one.
 * Metadata reads only: nothing is opened until the broadcast's acquirer runs.
 */
internal class AndroidLivePlatform(private val context: Context) : LivePlatform {
    override fun calibrate(): CalibrationOutcome =
        Calibrator.measure(AndroidClockSources.elapsedRealtime) { UnixMicros(System.currentTimeMillis().toULong() * 1_000uL) }

    override fun cameraFacts(): CaptureCapability {
        val manager = context.getSystemService(CameraManager::class.java) ?: return CaptureCapability.Unavailable(NO_CAMERA_SERVICE)
        val id =
            try {
                Camera2Port.backFacingId(manager)
            } catch (refused: CameraAccessException) {
                return CaptureCapability.Unavailable("the camera service refused the camera list: " + refused.reason)
            } ?: return CaptureCapability.Unavailable(NO_BACK_CAMERA)
        return PlatformFacts.cameraCapability(AndroidPlatformReaders.cameraTimestampSource(manager), id)
    }

    override fun audioFacts(): CaptureCapability = PlatformFacts.audioCapability(AndroidPlatformReaders.sdkLevel)

    override fun kit(late: LateCleanupSink): CaptureKit? =
        PlatformCapture.of(context, VIDEO, AUDIO, late)?.let { CaptureKit(it.dispatcher, it.video, it.audio, it.acquirer) }

    private companion object {
        const val NO_CAMERA_SERVICE = "this device has no camera service"
        const val NO_BACK_CAMERA = "this device has no back camera"

        /** 720p30 H.264 at 2 Mbit/s, a keyframe every two seconds. */
        val VIDEO: VideoEncoderConfiguration = planned(VideoEncoderConfiguration.of(1280, 720, 30, 2_000_000, 2_500_000uL, 2))

        /** 48 kHz mono AAC-LC at 96 kbit/s; mono is the input every microphone supports. */
        val AUDIO: AudioEncoderConfiguration = planned(AudioEncoderConfiguration.of(48_000, 1, 96_000, 128_000uL))

        fun <C : EncoderConfiguration> planned(plan: EncoderPlan<C>): C =
            when (plan) {
                is EncoderPlan.Planned -> plan.configuration
                is EncoderPlan.Refused -> throw IllegalStateException("the sample's encoder configuration is refused: " + plan.reason)
            }
    }
}

/** The library's endpoint and sender, exactly as the synthetic example drives them. */
internal object AndroidLiveEndpoint : LiveEndpoint<MoqEndpoint, MediaSender, SendTrack> {
    private val CONNECT_TIMEOUT = 10.seconds

    override suspend fun connect(config: EndpointConfig): MoqEndpoint = MoqEndpoint.connect(config, CONNECT_TIMEOUT)

    override suspend fun attach(endpoint: MoqEndpoint, config: SenderConfig): MediaSender = endpoint.attachSender(config)

    override suspend fun awaitReady(sender: MediaSender) = sender.awaitReady()

    override fun tracks(sender: MediaSender): TrackOperations<SendTrack> = sender.trackOperations()

    override fun completion(sender: MediaSender, endpoint: MoqEndpoint): CompletionSink = endpointCompletion(sender, endpoint)

    override suspend fun closeSender(sender: MediaSender) = sender.close()

    override suspend fun closeEndpoint(endpoint: MoqEndpoint) = endpoint.closeAndJoin()
}

/**
 * Late cleanup facts in the platform log. It outlives every session and the
 * Activity: a fact that arrives after its session has retired, or after the
 * screen is gone, is still recorded, under its own generation.
 */
internal object LogcatDiagnostics : LateDiagnostics {
    private const val TAG = "MoqPublisher"

    override fun record(fact: LateCleanupFact) {
        Log.w(TAG, "generation ${fact.generation}: ${fact.kind} of the ${fact.resource}" + (fact.failure?.let { " failed" } ?: ""), fact.failure)
    }
}
