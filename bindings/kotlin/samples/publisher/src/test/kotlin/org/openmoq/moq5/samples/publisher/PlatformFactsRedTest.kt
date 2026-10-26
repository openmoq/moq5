package org.openmoq.moq5.samples.publisher

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * The platform-fact reads, through injected seams.
 *
 * Nothing here opens a camera, starts a capture session, records audio, encodes
 * or asks for a permission: the two readings arrive as values, which is what
 * makes the DECISIONS testable on the host. The Android-backed readers exist in
 * production and are never invoked here.
 */
class PlatformFactsRedTest {

    private fun reader(value: Int) = CameraTimestampSourceReader { CameraTimestampSourceResult.Source(value) }

    private fun failing(reason: String) =
        CameraTimestampSourceReader { CameraTimestampSourceResult.Unavailable(reason) }

    @Test
    fun f1_aRealtimeCameraIsTheCalibratedBase() {
        assertEquals(
            TimebaseOutcome.Resolved(PlatformTimebase.ElapsedRealtime),
            PlatformFacts.cameraTimebase(reader(CameraTimestampSources.REALTIME), "0"),
        )
        assertEquals(
            CaptureCapability.Available,
            PlatformFacts.cameraCapability(reader(CameraTimestampSources.REALTIME), "0"),
        )
    }

    @Test
    fun f2_anUnknownSourceIsCarriedAsAFactAndRefusedForEpochPlacement() {
        assertEquals(
            TimebaseOutcome.Resolved(PlatformTimebase.Unknown),
            PlatformFacts.cameraTimebase(reader(CameraTimestampSources.UNKNOWN), "0"),
            "the fact is read; the refusal belongs to the accepted capability decision",
        )
        assertIs<CaptureCapability.Unavailable>(
            PlatformFacts.cameraCapability(reader(CameraTimestampSources.UNKNOWN), "0"),
        )
    }

    @Test
    fun f3_everyLookupFailureIsRefusedWithItsOwnReason() {
        // These are DIFFERENT platform outcomes and must not be collapsed into
        // "this device reports no timestamp source":
        //   * the characteristic really is absent;
        //   * the camera service refused the lookup (CameraAccessException);
        //   * the camera id does not exist (IllegalArgumentException);
        //   * policy denied it (SecurityException).
        val reasons = ArrayList<String>()
        for (why in listOf("absent", "access", "bad-id", "policy")) {
            val outcome = PlatformFacts.cameraTimebase(failing(why), "0")
            val unsupported = assertIs<TimebaseOutcome.Unsupported>(outcome, "not a REALTIME camera: " + why)
            assertTrue(unsupported.reason.isNotEmpty())
            assertTrue(unsupported.reason.contains(why), "the reader's own reason survives: " + unsupported.reason)
            reasons.add(unsupported.reason)
            assertIs<CaptureCapability.Unavailable>(PlatformFacts.cameraCapability(failing(why), "0"))
        }
        assertEquals(reasons.size, reasons.toSet().size, "four distinct reasons, not one label")
    }

    @Test
    fun f4_anUnrecognisedConstantIsRefused() {
        assertIs<TimebaseOutcome.Unsupported>(PlatformFacts.cameraTimebase(reader(7), "0"))
    }

    @Test
    fun f5_theApiLevelReadIsTheAcceptedDecision() {
        assertIs<CaptureCapability.Unavailable>(PlatformFacts.audioCapability(SdkLevelReader { 23 }))
        assertEquals(CaptureCapability.Available, PlatformFacts.audioCapability(SdkLevelReader { 24 }))
        assertEquals(
            CaptureCapabilities.audioTimestamps(23),
            PlatformFacts.audioCapability(SdkLevelReader { 23 }),
            "one decision, not a second copy of it",
        )
    }

    @Test
    fun f6_theCameraIsAskedExactlyOncePerDecision() {
        var calls = 0
        var lastId: String? = null
        val counting =
            CameraTimestampSourceReader { id ->
                calls++
                lastId = id
                CameraTimestampSourceResult.Source(CameraTimestampSources.REALTIME)
            }
        PlatformFacts.cameraTimebase(counting, "back-0")
        assertEquals(1, calls, "a metadata lookup, not a poll")
        assertEquals("back-0", lastId, "and for the camera it was asked about")

        calls = 0
        PlatformFacts.cameraCapability(counting, "back-0")
        assertEquals(1, calls, "the capability decision reads it once too, not twice")
    }

    @Test
    fun f7_theConstantsAreThePlatformsOwn() {
        // 1775 carried these as documented values because the gate refused the
        // package; they now come from CameraMetadata itself.
        assertEquals(0, CameraTimestampSources.UNKNOWN)
        assertEquals(1, CameraTimestampSources.REALTIME)
    }
}
