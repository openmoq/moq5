package org.openmoq.moq5.samples.publisher

import android.hardware.camera2.CameraAccessException
import android.hardware.camera2.CameraCharacteristics
import android.hardware.camera2.CameraManager
import android.hardware.camera2.CameraMetadata
import android.os.Build

/**
 * Reading the platform facts that NAME the capture clock, and nothing else.
 *
 * This is an acquisition-facts seam: it asks the camera which timestamp source
 * it stamps frames on, and asks the running build whether an `AudioTimestamp`
 * is available at all. It never opens a camera, starts a capture session,
 * records audio, encodes, or asks for a permission — those are separate,
 * still-held boundaries.
 *
 * Both readings arrive through injectable seams, so every decision below is
 * host-testable with no device: the Android-backed implementations are supplied
 * as defaults and are never invoked by a host test.
 */
/**
 * What a metadata lookup produced.
 *
 * "The device does not report this characteristic" and "the lookup failed" are
 * DIFFERENT facts, and a real lookup can fail in several ways — the camera
 * service refusing (`CameraAccessException`), an id that does not exist
 * (`IllegalArgumentException`), or policy denying it (`SecurityException`). Each
 * carries its own reason so a refusal is never mislabelled as an absent
 * characteristic.
 */
internal sealed interface CameraTimestampSourceResult {
    data class Source(val constant: Int) : CameraTimestampSourceResult

    data class Unavailable(val reason: String) : CameraTimestampSourceResult
}

internal fun interface CameraTimestampSourceReader {
    /** `SENSOR_INFO_TIMESTAMP_SOURCE` for the named camera, or why not. */
    fun sourceOf(cameraId: String): CameraTimestampSourceResult
}

internal fun interface SdkLevelReader {
    fun sdkInt(): Int
}

/** The platform's own readers. Never called by a host test. */
internal object AndroidPlatformReaders {
    /**
     * Metadata lookup only: this never opens a camera. Each documented failure
     * of the lookup becomes its own named result rather than escaping into the
     * session or being flattened into "absent".
     */
    fun cameraTimestampSource(manager: CameraManager): CameraTimestampSourceReader =
        CameraTimestampSourceReader { id ->
            try {
                val source = manager.getCameraCharacteristics(id).get(CameraCharacteristics.SENSOR_INFO_TIMESTAMP_SOURCE)
                if (source == null) {
                    CameraTimestampSourceResult.Unavailable(
                        "camera " + id + " reports no SENSOR_INFO_TIMESTAMP_SOURCE characteristic",
                    )
                } else {
                    CameraTimestampSourceResult.Source(source)
                }
            } catch (refused: CameraAccessException) {
                CameraTimestampSourceResult.Unavailable(
                    "the camera service refused the metadata lookup for " + id + ": " + refused.reason,
                )
            } catch (unknown: IllegalArgumentException) {
                CameraTimestampSourceResult.Unavailable("there is no camera " + id + " on this device")
            } catch (denied: SecurityException) {
                CameraTimestampSourceResult.Unavailable("policy denied the metadata lookup for camera " + id)
            }
        }

    val sdkLevel: SdkLevelReader = SdkLevelReader { Build.VERSION.SDK_INT }
}

/**
 * The platform constants, from the platform itself. 1775 carried these as
 * documented values because the sample gate refused the camera2 package; with
 * the bounded publisher-only widening they are read from `CameraMetadata`, so a
 * future renumbering cannot drift from this sample.
 */
internal object CameraTimestampSources {
    const val UNKNOWN: Int = CameraMetadata.SENSOR_INFO_TIMESTAMP_SOURCE_UNKNOWN
    const val REALTIME: Int = CameraMetadata.SENSOR_INFO_TIMESTAMP_SOURCE_REALTIME
}

/** The platform facts this sample acts on, as accepted values. */
internal object PlatformFacts {
    /**
     * What the named camera's timestamps are on, mapped through the accepted
     * contract. A device that reports nothing is a named refusal, not a guess.
     */
    fun cameraTimebase(reader: CameraTimestampSourceReader, cameraId: String): TimebaseOutcome {
        val source =
            when (val result = reader.sourceOf(cameraId)) {
                is CameraTimestampSourceResult.Source -> result.constant
                // The reader's own reason survives: an unavailable lookup is
                // never relabelled as an absent characteristic.
                is CameraTimestampSourceResult.Unavailable -> return TimebaseOutcome.Unsupported(result.reason)
            }
        // The mapping itself is the accepted one, so this seam adds a read and
        // nothing else.
        return AndroidCaptureFacts.cameraTimebase(source)
    }

    /** Whether this camera's clock can carry an epoch capture instant at all. */
    fun cameraCapability(reader: CameraTimestampSourceReader, cameraId: String): CaptureCapability =
        when (val timebase = cameraTimebase(reader, cameraId)) {
            is TimebaseOutcome.Resolved -> CaptureCapabilities.cameraTimestamps(timebase.timebase)
            is TimebaseOutcome.Unsupported -> CaptureCapability.Unavailable(timebase.reason)
        }

    /** The running build's audio-timestamp capability, through the accepted decision. */
    fun audioCapability(reader: SdkLevelReader): CaptureCapability =
        CaptureCapabilities.audioTimestamps(reader.sdkInt())
}
