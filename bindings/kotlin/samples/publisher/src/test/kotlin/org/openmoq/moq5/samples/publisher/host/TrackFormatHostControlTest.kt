package org.openmoq.moq5.samples.publisher.host

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.CodecSignaling
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.SampleEntry

/**
 * The independent native controls for the format validator's host rows,
 * through the public binding directly, in their own JVM (the `testHostCodec`
 * task runs one JVM per class). Nothing here goes through the sample.
 *
 * The host image's codec recorder holds 16 native calls per JVM; this class
 * makes 10: t0 2 (size query and exact call), t5 8 (two builds and two
 * formats, 2 each).
 */
class TrackFormatHostControlTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private fun cat(vararg parts: ByteArray): ByteArray = parts.fold(ByteArray(0)) { a, b -> a + b }

    private val sc = hex("00000001")
    private val spsA = hex("6742001eabcd")
    private val asc441 = hex("1210")

    @Test
    fun `t0 control - the public binding formats the AAC LC string in this JVM`() {
        assertEquals(
            "mp4a.40.2",
            CodecSignaling.formatCodecString(CodecConfigFormat.AacAudioSpecificConfig, SampleEntry("mp4a"), MediaBytes(asc441), 0x40),
        )
    }

    @Test
    fun `t5 control - the real builder gives two same-length PPS two distinct records`() {
        val first = CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(cat(sc, spsA, sc, hex("68ce3c81"))), 4).bytes()
        val second = CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(cat(sc, spsA, sc, hex("68ce3c82"))), 4).bytes()
        assertEquals(first.size, second.size)
        assertContentEquals(hex("0142001effe100066742001eabcd01000468ce3c81"), first)
        assertContentEquals(hex("0142001effe100066742001eabcd01000468ce3c82"), second)
        // A PPS-only change leaves the profile and level, so both records format to one declared string.
        assertEquals("avc1.42001e", CodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), MediaBytes(first)))
        assertEquals("avc1.42001e", CodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), MediaBytes(second)))
    }
}
