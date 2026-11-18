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
import kotlin.test.assertNotNull
import kotlin.test.assertNull

/**
 * Declared framing fixtures, not decodable pictures. avcC's extension count
 * requires that many length-framed entries, just as its SPS/PPS counts do.
 * LOC-01 2.1.3/2.1.4 disambiguates a start code from a length; a start code
 * still precedes a NAL unit and cannot itself be a decoder unit.
 */
class PlayerFramingValidationTest {
    private fun hex(text: String) = text.split(' ').map { it.toInt(16).toByte() }.toByteArray()

    private val legacy = hex("01 64 00 1f ff e1 00 0a 67 64 00 1f ac d9 40 50 05 bb 01 00 04 68 eb e3 cb")
    private val complete = legacy + hex("fd f8 f8 00")

    @Test
    fun legacyAndCompleteExtensionsKeepTheirDeclaredBaseConfiguration() {
        val expectedSps = hex("67 64 00 1f ac d9 40 50 05 bb")
        val expectedPps = hex("68 eb e3 cb")
        for (record in listOf(legacy, complete, legacy + hex("fd f8 f8 01 00 02 6d 80"), legacy + hex("fd f8 f8 02 00 02 6d 80 00 02 6d 81"))) {
            val parsed = assertNotNull(Avc.parseConfigOrNull(record))
            assertEquals(4, parsed.nalLengthSize)
            assertEquals(100, parsed.profile)
            assertEquals(1, parsed.sequenceParameterSets.size)
            assertEquals(1, parsed.pictureParameterSets.size)
            assertContentEquals(expectedSps, parsed.sequenceParameterSets.single())
            assertContentEquals(expectedPps, parsed.pictureParameterSets.single())
        }
    }

    @Test
    fun aDeclaredExtensionNeedsBothLengthBytes() {
        for (tail in listOf("fd f8 f8 01", "fd f8 f8 01 00")) {
            assertNull(Avc.parseConfigOrNull(legacy + hex(tail)), "missing extension length: $tail")
        }
    }

    @Test
    fun aDeclaredExtensionNeedsItsEntireNonemptyPayload() {
        for (tail in listOf("fd f8 f8 01 00 00", "fd f8 f8 01 00 02", "fd f8 f8 01 00 02 6d")) {
            assertNull(Avc.parseConfigOrNull(legacy + hex(tail)), "missing extension payload: $tail")
        }
    }

    @Test
    fun everyDeclaredExtensionIsCheckedNotJustTheFirst() {
        assertNull(Avc.parseConfigOrNull(legacy + hex("fd f8 f8 02 00 02 6d 80 00 02 6d")))
    }

    private fun format() = assertIs<FormatOutcome.Video>(
        DecoderFormats.of(TrackFacts(MediaType.Video, PackagingKind.Raw, "avc1.64001f", complete, width = 1280u, height = 720u)),
    )

    private fun facts(bytes: ByteArray) = ObjectFacts(
        bytes, ObjectStatus.Normal, true, MediaMicros(1000u), MediaMicros(1000u), MediaOffsetMicros(0), UnixMicros(1000u),
    )

    @Test
    fun aDelimiterWithoutANalDoesNotCountAsMedia() {
        val decoded = PlaybackDecode(format())
        decoded.accept(facts(hex("00 00 00 01")))
        assertEquals(0, decoded.units)
        assertEquals(1, decoded.rejected)
    }

    @Test
    fun emptyLeadingIntermediateAndTrailingNalUnitsRejectTheWholePayload() {
        for (payload in listOf(
            "00 00 00 01 00 00 00 01 65 80",
            "00 00 00 01 65 80 00 00 01 00 00 00 01 61 80",
            "00 00 00 01 65 80 00 00 00 01",
            "00 00 00 01 65 80 00 00 01",
            "00 00 00 01 00 00",
        )) {
            assertIs<AccessUnitOutcome.Rejected>(AccessUnits.of(format(), facts(hex(payload))), payload)
        }
    }

    @Test
    fun completeNalUnitsRetainTheirBytesIncludingEscapesAndTrailingZeros() {
        for (payload in listOf(
            "00 00 00 01 65 80",
            "00 00 00 01 65 00 00 03 01 80 00 00 00 01 61 80",
            "00 00 00 01 65 80 00 00 01 61 80 00 00",
        )) {
            val bytes = hex(payload)
            val original = bytes.copyOf()
            val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(format(), facts(bytes)))
            assertContentEquals(original, unit.bytes)
            assertContentEquals(original, bytes)
            assertEquals(1000L, unit.presentationTimeUs)
        }
    }

    @Test
    fun lengthPrefixedCarriageAndLocValueOneDisambiguationRemainDistinct() {
        val bytes = hex("00 00 00 02 65 80")
        val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(format(), facts(bytes)))
        assertContentEquals(hex("00 00 00 01 65 80"), unit.bytes)
        val one = hex("00 00 00 01 65")
        assertEquals(VideoFraming.AnnexB, Avc.framingOf(one, format().avc))
        assertContentEquals(one, assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(format(), facts(one))).bytes)
        val cmaf = format().copy(packaging = PackagingKind.Cmaf, framing = FramingPolicy.StrictLengthPrefixed)
        assertContentEquals(one, assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(cmaf, facts(one))).bytes)
    }
}
