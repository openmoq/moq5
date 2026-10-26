package org.openmoq.moq5.samples.player

import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

/** ADTS framing fixtures, not decoded AAC or verified CRC checksums. */
class PlayerAdtsFramingTest {
    private fun hex(text: String) = text.split(' ').map { it.toInt(16).toByte() }.toByteArray()

    // LC, 44100 Hz, two channels, one raw_data_block. The lengths include the
    // seven-byte header, optional two-byte CRC, and the three media bytes.
    private val unprotected = hex("ff f1 50 80 01 5f fc 21 22 23")
    private val protected = hex("ff f0 50 80 01 9f fc aa bb 21 22 23")

    private fun format(mode: AacBitstream = AacBitstream.Adts) = assertIs<FormatOutcome.Audio>(
        DecoderFormats.of(TrackFacts(MediaType.Audio, PackagingKind.Raw, "mp4a.40.2", hex("12 10"), aacBitstream = mode)),
    )

    private fun facts(bytes: ByteArray) = ObjectFacts(
        bytes, ObjectStatus.Normal, true, MediaMicros(1000u), MediaMicros(1000u), MediaOffsetMicros(0), UnixMicros(1000u),
    )

    @Test
    fun bothSingleBlockHeaderFormsStripOnlyTheirHeader() {
        for (frame in listOf(unprotected, protected)) {
            val original = frame.copyOf()
            assertContentEquals(hex("21 22 23"), Aac.stripAdtsOrNull(frame))
            val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(format(), facts(frame)))
            assertContentEquals(hex("21 22 23"), unit.bytes)
            assertEquals(1000L, unit.presentationTimeUs)
            assertContentEquals(original, frame)
        }
    }

    @Test
    fun eachMultiblockCountIsRefusedEvenWithoutCrc() {
        // The two-bit field stores block count minus one. This converter has
        // no raw_data_block parser and must not call their concatenation one AU.
        for (countMinusOne in 1..3) {
            val frame = unprotected.copyOf().also { it[6] = (0xfc or countMinusOne).toByte() }
            assertNull(Aac.stripAdtsOrNull(frame), "block count minus one = $countMinusOne")
            assertIs<AccessUnitOutcome.Rejected>(AccessUnits.of(format(), facts(frame)))
        }
    }

    @Test
    fun crcMultiblockPositionBytesAreNeverReturnedAsMedia() {
        val truncated = hex("ff f0 50 80 01 5f fd 00 04 aa")
        assertNull(Aac.stripAdtsOrNull(truncated), "block-position and CRC framing is not a nine-byte header")
        val decoded = PlaybackDecode(format())
        decoded.accept(facts(truncated))
        assertEquals(0, decoded.units)
        assertEquals(1, decoded.rejected)
        for (countMinusOne in 1..3) {
            val frame = protected.copyOf().also { it[6] = (0xfc or countMinusOne).toByte() }
            assertNull(Aac.stripAdtsOrNull(frame), "protected block count minus one = $countMinusOne")
        }
    }

    @Test
    fun rawCarriageDoesNotSniffAnAdtsHeader() {
        val multiple = unprotected.copyOf().also { it[6] = 0xfd.toByte() }
        val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(format(AacBitstream.Raw), facts(multiple)))
        assertContentEquals(multiple, unit.bytes)
    }

    @Test
    fun reservedAdtsFrequencyIndicesAreRefused() {
        // ADTS has no explicit-frequency escape: its four-bit field must
        // select an actual frequency, indices 0..12, not 13/14/15.
        for (base in listOf(unprotected, protected)) for (index in 13..15) {
            val frame = base.copyOf().also { it[2] = (0x40 or (index shl 2)).toByte() }
            assertNull(Aac.stripAdtsOrNull(frame), "reserved ADTS frequency index $index")
            assertIs<AccessUnitOutcome.Rejected>(AccessUnits.of(format(), facts(frame)))
        }
    }

    @Test
    fun everyDefinedAdtsFrequencyIndexStillHasValidFraming() {
        for (index in 0..12) {
            val frame = unprotected.copyOf().also { it[2] = (0x40 or (index shl 2)).toByte() }
            assertContentEquals(hex("21 22 23"), Aac.stripAdtsOrNull(frame), "frequency index $index")
        }
    }

    @Test
    fun singleBlockStillRequiresAnEntireHeaderAndPayloadAndExactFrameLength() {
        for (frame in listOf(
            protected.copyOf(8),
            hex("ff f0 50 80 01 3f fc aa bb"),
            protected + byteArrayOf(0x44),
            unprotected.copyOf(9),
        )) assertNull(Aac.stripAdtsOrNull(frame))
    }
}
