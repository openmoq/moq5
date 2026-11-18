package org.openmoq.moq5.samples.player

import org.openmoq.moq5.MediaType
import org.openmoq.moq5.PackagingKind
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** Configuration field geometry; no compressed audio or Android decoder is exercised. */
class PlayerAacConfigurationTest {
    private fun hex(text: String) = text.split(' ').map { it.toInt(16).toByte() }.toByteArray()

    private fun format(bytes: ByteArray) =
        DecoderFormats.of(TrackFacts(MediaType.Audio, PackagingKind.Raw, "mp4a.40.2", bytes))

    private fun accepted(text: String, rate: Int = 44_100, channels: Int = 2) {
        val bytes = hex(text)
        val original = bytes.copyOf()
        assertEquals(AacConfig(2, rate, channels), assertIs<AacParse.Ok>(Aac.parse(bytes)).config)
        val audio = assertIs<FormatOutcome.Audio>(format(bytes))
        assertEquals(AacConfig(2, rate, channels), audio.aac)
        assertContentEquals(original, audio.csd.single())
        assertContentEquals(original, bytes)
    }

    private fun refused(text: String) {
        val bytes = hex(text)
        assertIs<AacParse.No>(Aac.parse(bytes), text)
        assertIs<FormatOutcome.Unsupported>(format(bytes), text)
    }

    @Test
    fun missingConditionalCoreCoderDelayIsRefused() {
        refused("12 12")
        refused("12 12 03")
    }

    @Test
    fun missingConditionalGaExtensionTailIsRefused() {
        refused("12 11")
    }

    @Test
    fun syncExtensionSbrCannotMasqueradeAsPlainLcAtItsCoreRate() {
        // LC core 22050 Hz, channelConfiguration 2; sync 0x2b7, AOT 5,
        // sbrPresent=1, extension frequency index 4 (44100 Hz).
        refused("13 90 56 e5 a0")
        refused("12 10 56 e5 a0")
        // Extension rate index 0 (96000 Hz) leaves only zero bits after the
        // flag: a generic nonzero-tail refusal cannot stand in for SBR policy.
        refused("12 10 56 e5 80")
    }

    @Test
    fun completeGaFieldsAndExplicitlyAbsentSbrPreserveLcAndCsd() {
        for (text in listOf(
            "12 10",             // 1024-frame LC, no conditional GA fields.
            "12 14",             // frameLengthFlag=1; timing is not derived here.
            "12 12 03 d8",       // dependsOnCoreCoder=1, delay=123, extensionFlag=0.
            "12 11 00",          // extensionFlag=1, extensionFlag3=0.
            "12 12 03 dc",       // delay=123, extensionFlag=1, extensionFlag3=0.
            "12 10 56 e5 00",    // sync 0x2b7, AOT 5, sbrPresent=0, zero padding.
            "12 12 03 d9 5b 94", // sync begins at bit 30 after conditional delay.
            "12 11 2b 72 80",    // sync begins at bit 17 after extensionFlag3.
            "12 10 00 00",       // zero padding changes no configuration facts.
        )) accepted(text)
    }

    @Test
    fun explicitlyAbsentPsAfterAbsentSbrPreservesLcAndCsd() {
        // After the 16-bit LC config: sync 0x2b7 (11), AOT 5 (5),
        // SBR absent (1), PS sync 0x548 (11), PS absent (1), zero padding.
        accepted("12 10 56 e5 54 80")
        accepted("12 10 56 e5 54 80 00")
    }

    @Test
    fun psPresentTruncatedOrUnknownTailIsNotPlainLc() {
        refused("12 10 56 e5 54 88") // PS flag 1.
        refused("12 10 56 e5 54") // incomplete PS sync marker.
        refused("12 10 56 e5 54 90") // wrong PS marker.
        refused("12 10 56 e5 54 81") // PS absent followed by unknown data.
    }

    @Test
    fun allPreviouslySupportedFrequencyAndChannelConfigurationsRemain() {
        val rates = intArrayOf(96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000, 7_350)
        // Declared ASC header fields: AOT 2 (5), rate index (4), channels (4), GA 000 (3).
        for ((index, rate) in rates.withIndex()) for (channelConfig in 1..7) {
            val header = (2 shl 11) or (index shl 7) or (channelConfig shl 3)
            val bytes = byteArrayOf((header ushr 8).toByte(), header.toByte())
            assertEquals(AacConfig(2, rate, if (channelConfig == 7) 8 else channelConfig), assertIs<AacParse.Ok>(Aac.parse(bytes)).config)
        }
    }

    @Test
    fun unsupportedExtensionSyntaxIsNotIgnored() {
        refused("12 11 80") // extensionFlag3 set: unknown further GA syntax.
        refused("12 10 56 e6 00") // sync extension AOT 6, not plain LC.
        refused("12 10 56 e5 40") // SBR absent but a nonzero unparsed tail.
        refused("12 10 01") // not zero padding or a supported extension.
    }

    @Test
    fun incompleteSyncExtensionsAreNotPadding() {
        refused("12 10 56") // only eight of the eleven sync bits.
        refused("12 10 56 e5") // complete sync/type, no sbrPresent bit.
    }

    @Test
    fun otherObjectTypesFrequencyEscapesAndProgramConfigStillRefuse() {
        for (text in listOf("0a 10", "2a 10", "f8 10", "17 90", "16 90", "12 00", "12 40")) refused(text)
        assertIs<AacParse.No>(Aac.parse(byteArrayOf()))
        assertIs<AacParse.No>(Aac.parse(byteArrayOf(0x12)))
    }
}
