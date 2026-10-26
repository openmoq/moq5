package org.openmoq.moq5.samples.player

import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The playback loop's per-object decision, now on the accepted media contract.
 *
 * These rows are what makes the contract the code path rather than a value model
 * beside it: every count below comes from `AccessUnits`, not from the object's
 * own byte length.
 */
class PlaybackDecodeRedTest {

    private val sps = byteArrayOf(0x67, 0x64, 0x00, 0x1f, 0xac.toByte(), 0xd9.toByte(), 0x40, 0x50, 0x05, 0xbb.toByte())
    private val pps = byteArrayOf(0x68, 0xeb.toByte(), 0xe3.toByte(), 0xcb.toByte())

    private val avcConfig =
        AvcConfig(listOf(sps), listOf(pps), nalLengthSize = 4, profile = 0x64, constraints = 0x00, level = 0x1f)

    private val format =
        FormatOutcome.Video(
            mime = "video/avc",
            width = 1280,
            height = 720,
            csd = listOf(MediaContract.START_CODE + sps, MediaContract.START_CODE + pps),
            avc = avcConfig,
            packaging = PackagingKind.Cmaf,
            framing = FramingPolicy.StrictLengthPrefixed,
            timescale = 90_000u,
        )

    /** Two length-prefixed NALs: nine bytes then seven. */
    private val lengthPrefixed =
        byteArrayOf(0x00, 0x00, 0x00, 0x05, 0x65, 0x01, 0x02, 0x03, 0x04) +
            byteArrayOf(0x00, 0x00, 0x00, 0x03, 0x41, 0xaa.toByte(), 0xbb.toByte())

    private val syncFlags = 0x02000000u
    private val nonSyncFlags = 0x01010000u

    private fun sample(size: Int, cts: Int = 0, flags: UInt = syncFlags) =
        SampleFacts(size.toUInt(), 3_000u, cts, flags)

    private fun obj(
        payload: ByteArray,
        status: ObjectStatus = ObjectStatus.Normal,
        keyframe: Boolean = true,
        decodeUs: Long = 1_000_000,
        samples: List<SampleFacts> = listOf(sample(payload.size)),
    ) = ObjectFacts(
        payload = payload,
        status = status,
        keyframe = keyframe,
        presentationTime = MediaMicros(decodeUs.toULong()),
        decodeTime = MediaMicros(decodeUs.toULong()),
        compositionOffset = MediaOffsetMicros(0),
        captureTime = UnixMicros(1_746_104_600_000_000uL),
        samples = samples,
    )

    @Test
    fun w1_aFragmentIsCountedAsAccessUnitsNotAsAPayload() {
        val decode = PlaybackDecode(format)
        decode.accept(obj(lengthPrefixed, samples = listOf(sample(9), sample(7, cts = 3_000, flags = nonSyncFlags))))
        assertEquals(2, decode.units, "the sample table decides, not the object")
        assertEquals(16L, decode.unitBytes, "each prefix became a start code: the framed size, not the mdat size")
        assertEquals(1, decode.syncUnits, "the second sample's own flags")
        // The second sample: one 3000-tick step from the base, plus its own
        // 3000-tick composition offset.
        assertEquals(1_066_666L, decode.lastPresentationUs, "the cumulative tick conversion, not a byte count")
        assertEquals(0, decode.rejected)
        assertEquals(0, decode.statusObjects)
    }

    @Test
    fun w2_aStatusObjectIsNeverCountedAsMedia() {
        val decode = PlaybackDecode(format)
        decode.accept(obj(ByteArray(0), status = ObjectStatus.EndOfGroup, keyframe = false, samples = emptyList()))
        assertEquals(0, decode.units, "an end-of-group object carries no media")
        assertEquals(1, decode.statusObjects)
        assertEquals(0L, decode.unitBytes)
        assertNull(decode.lastPresentationUs)
    }

    @Test
    fun w3_aRefusedObjectIsCountedAndNamedNotQueued() {
        val decode = PlaybackDecode(format)
        // A sample that runs past the media bytes: the contract refuses the
        // whole object rather than exposing a prefix.
        decode.accept(obj(lengthPrefixed, samples = listOf(sample(9), sample(99))))
        assertEquals(0, decode.units, "nothing is queued from a refused object")
        assertEquals(1, decode.rejected)
        assertNotNull(decode.firstRejection)
        assertTrue(decode.firstRejection!!.isNotEmpty())
    }

    @Test
    fun w4_theSummaryReportsUnitsRatherThanRawBytes() {
        val decode = PlaybackDecode(format)
        decode.accept(obj(lengthPrefixed, samples = listOf(sample(9), sample(7, cts = 3_000))))
        decode.accept(obj(ByteArray(0), status = ObjectStatus.EndOfGroup, keyframe = false, samples = emptyList()))
        val line = decode.summary()
        assertTrue(line.contains("2"), line)
        assertTrue(line.contains("access unit", ignoreCase = true), line)
        assertTrue(line.contains("16"), "the framed byte count, not the payload's: " + line)
    }

    @Test
    fun w5_anUnsupportedFormatQueuesNothingAndSaysWhy() {
        val decode = PlaybackDecode(FormatOutcome.Unsupported("vp09 is not in this sample's subset"))
        decode.accept(obj(lengthPrefixed))
        assertEquals(0, decode.units)
        assertEquals(1, decode.rejected)
        assertTrue(decode.summary().isNotEmpty())
    }
}
