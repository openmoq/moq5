package org.openmoq.moq5.samples.publisher

import android.Manifest
import android.content.Context
import android.content.pm.PackageManager
import android.hardware.camera2.CameraCaptureSession
import android.hardware.camera2.CameraCharacteristics
import android.hardware.camera2.CameraDevice
import android.hardware.camera2.CameraManager
import android.hardware.camera2.CaptureRequest
import android.util.Range
import android.view.Surface

/**
 * The platform [CameraPort]: one Camera2 camera, its callbacks posted to the
 * owned worker's [HandlerDispatcher]. Every decision -- tickets, adoption,
 * refusal, loss, the repeating request's timing -- is [CameraPart]'s; this
 * file holds only the Camera2 calls.
 *
 * `openCamera` requires the CAMERA permission. The session's permission flow
 * refuses a start without it; the check here is the platform's own, made in
 * the same call so a revoked grant fails where it stands.
 */
internal class Camera2Port(
    private val context: Context,
    private val manager: CameraManager,
    private val cameraId: String,
    private val dispatcher: HandlerDispatcher,
) : CameraPort {
    override fun facts(): CameraFacts {
        val characteristics = manager.getCameraCharacteristics(cameraId)
        val sizes =
            characteristics.get(CameraCharacteristics.SCALER_STREAM_CONFIGURATION_MAP)
                ?.getOutputSizes(encoderSurfaceClass)
                ?.map { it.width to it.height }
                .orEmpty()
        val ranges =
            characteristics.get(CameraCharacteristics.CONTROL_AE_AVAILABLE_TARGET_FPS_RANGES)
                ?.map { it.lower..it.upper }
                .orEmpty()
        return CameraFacts(sizes, ranges)
    }

    override fun open(events: CameraEvents) {
        if (context.checkSelfPermission(Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            throw SecurityException("the CAMERA permission is not granted")
        }
        manager.openCamera(cameraId, DeviceCallback(events, dispatcher), dispatcher.handler())
    }

    companion object {
        /** The first back-facing camera, or null when the device has none. */
        fun backFacing(context: Context, dispatcher: HandlerDispatcher): Camera2Port? {
            val manager = context.getSystemService(CameraManager::class.java) ?: return null
            val id = backFacingId(manager) ?: return null
            return Camera2Port(context, manager, id, dispatcher)
        }

        /** The id [backFacing] opens, by the same metadata query, or null when the device has none. */
        fun backFacingId(manager: CameraManager): String? =
            manager.cameraIdList.firstOrNull {
                manager.getCameraCharacteristics(it).get(CameraCharacteristics.LENS_FACING) == CameraCharacteristics.LENS_FACING_BACK
            }
    }
}

/** `CameraDevice.StateCallback`, handing the device over wrapped; the wrapper's identity is the device. */
private class DeviceCallback(private val events: CameraEvents, private val dispatcher: HandlerDispatcher) : CameraDevice.StateCallback() {
    override fun onOpened(camera: CameraDevice) = events.onOpened(DeviceLink(camera, dispatcher))

    override fun onDisconnected(camera: CameraDevice) = events.onDisconnected(DeviceLink(camera, dispatcher))

    override fun onError(camera: CameraDevice, error: Int) = events.onError(DeviceLink(camera, dispatcher), error)

    override fun onClosed(camera: CameraDevice) = events.onClosed(DeviceLink(camera, dispatcher))
}

private class DeviceLink(private val device: CameraDevice, private val dispatcher: HandlerDispatcher) : CameraLink {
    override val identity: Any get() = device

    override fun close() = device.close()

    // The List<Surface> overload is the one session factory below API 28; it
    // is deprecated from API 30 but not removed, and this sample's floor is 23.
    @Suppress("DEPRECATION")
    override fun createStream(surface: Any, events: StreamEvents) =
        device.createCaptureSession(listOf(surface as Surface), SessionCallback(device, events, dispatcher), dispatcher.handler())
}

/** `CameraCaptureSession.StateCallback`, handing the session over wrapped; the wrapper's identity is the session. */
private class SessionCallback(
    private val device: CameraDevice,
    private val events: StreamEvents,
    private val dispatcher: HandlerDispatcher,
) : CameraCaptureSession.StateCallback() {
    override fun onConfigured(session: CameraCaptureSession) = events.onConfigured(SessionLink(session, device, dispatcher))

    override fun onConfigureFailed(session: CameraCaptureSession) = events.onConfigureFailed(SessionLink(session, device, dispatcher))
}

private class SessionLink(
    private val session: CameraCaptureSession,
    private val device: CameraDevice,
    private val dispatcher: HandlerDispatcher,
) : StreamLink {
    override val identity: Any get() = session

    override fun close() = session.close()

    override fun startRepeating(surface: Any, fps: IntRange) {
        val request =
            device.createCaptureRequest(CameraDevice.TEMPLATE_RECORD).apply {
                addTarget(surface as Surface)
                set(CaptureRequest.CONTROL_AE_TARGET_FPS_RANGE, Range(fps.first, fps.last))
            }.build()
        session.setRepeatingRequest(request, null, dispatcher.handler())
    }
}

/**
 * The real capture environment's platform half: the owned worker, the video
 * encoder over `MediaCodec` and the back camera, the audio encoder and the
 * microphone, and the one acquirer that sequences them. The live launcher's
 * [AndroidLivePlatform] builds it and composes it into the broadcast.
 */
internal class PlatformCapture private constructor(
    val dispatcher: HandlerDispatcher,
    val video: TrackEncoder,
    val audio: TrackEncoder,
    val acquirer: SequencedAcquirer,
) {
    companion object {
        /** Null when the device has no back camera. Creates no thread and opens nothing. */
        fun of(context: Context, video: VideoEncoderConfiguration, audio: AudioEncoderConfiguration, late: LateCleanupSink): PlatformCapture? {
            val dispatcher = HandlerDispatcher("capture")
            val camera = Camera2Port.backFacing(context, dispatcher) ?: return null
            val videoEncoder = TrackEncoder(video, MediaCodecBackend(dispatcher))
            val audioEncoder = TrackEncoder(audio, MediaCodecBackend(dispatcher))
            val acquirer =
                SequencedAcquirer(
                    WorkerStart { dispatcher.startWorker() },
                    CameraPart(videoEncoder, camera, late),
                    AudioPart(audioEncoder, AudioRecordPort(context, dispatcher), dispatcher, dispatcher, late),
                )
            return PlatformCapture(dispatcher, videoEncoder, audioEncoder, acquirer)
        }
    }
}
