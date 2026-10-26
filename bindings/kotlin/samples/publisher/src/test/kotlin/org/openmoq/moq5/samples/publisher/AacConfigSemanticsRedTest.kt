package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotEquals

/**
 * The sample's AAC configuration semantics over an owned [RawConfigSnapshot].
 *
 * Every vector is a literal whose bits were laid out by hand, field by field:
 * audioObjectType(5) samplingFrequencyIndex(4) [samplingFrequency(24)]
 * channelConfiguration(4), then frameLengthFlag, dependsOnCoreCoder and
 * extensionFlag (1 each), then any 0x2b7 extension. `g` rows are this file's
 * own controls: [bits] re-derives each literal independently of the code
 * under test. `a` rows measure the real [AacConfigSemantics.examine]; `k`
 * rows the keying it hands on; `b` the bound it reads within.
 *
 * Refused vectors are of two kinds, and the helpers name which. COMPLETE
 * vectors are whole configurations outside the supported subset. PREFIX
 * probes stop at a feature whose continuation the sample never parses; they
 * are NOT complete streams (1192 carries no core-coder delay, 1180 no program
 * config element), so a refusal certifies nothing about what would follow.
 * Expected frequencies are literals, never read from the product's table.
 */
class AacConfigSemanticsRedTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    /** Packs (value, width) fields most-significant-bit first, zero-padding the last byte. Test-only; shares nothing with the parser. */
    private fun bits(vararg fields: Pair<Long, Int>): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        var acc = 0
        var n = 0
        for ((value, width) in fields) {
            for (i in width - 1 downTo 0) {
                acc = (acc shl 1) or ((value ushr i) and 1L).toInt()
                if (++n == 8) {
                    out.write(acc)
                    acc = 0
                    n = 0
                }
            }
        }
        if (n > 0) out.write(acc shl (8 - n))
        return out.toByteArray()
    }

    private fun lc(aot: Long = 2, sfi: Long = 3, ch: Long = 2, fl: Long = 0, dcc: Long = 0, ext: Long = 0) =
        arrayOf(aot to 5, sfi to 4, ch to 4, fl to 1, dcc to 1, ext to 1)

    private val bounds = ConfigBounds(maxUnits = 4, maxUnitBytes = 4096, maxTotalBytes = 8192)

    private fun snapshot(vararg units: ByteArray): RawConfigSnapshot =
        assertIs<RawSnapshotResult.Taken>(RawConfigSnapshots.capture(units.map { ByteBuffer.wrap(it) }, bounds), "setup: the snapshot is taken").snapshot

    private val stereo48 = DeclaredAudioFacts(48_000, 2)
    private val mono48 = DeclaredAudioFacts(48_000, 1)

    private fun examine(hexBytes: String, declared: DeclaredAudioFacts = stereo48) = AacConfigSemantics.examine(snapshot(hex(hexBytes)), declared)

    private fun supported(hexBytes: String, declared: DeclaredAudioFacts, rate: Int, channels: Int, frames: Int) {
        val facts = assertIs<AacConfigVerdict.Supported>(examine(hexBytes, declared), "0x$hexBytes is supported").facts
        assertEquals(rate, facts.sampleRate, "0x$hexBytes: the ASC's own sampling frequency")
        assertEquals(channels, facts.channelCount, "0x$hexBytes: the ASC's own channel count")
        assertEquals(frames, facts.framesPerAccessUnit, "0x$hexBytes: samples per access unit, from frameLengthFlag")
    }

    private fun refused(hexBytes: String, what: String, features: List<AscFeature>, declared: DeclaredAudioFacts) {
        val v = assertIs<AacConfigVerdict.Unsupported>(examine(hexBytes, declared), "0x$hexBytes: $what, refused as outside the subset")
        assertEquals(features, v.features, "0x$hexBytes: every feature found along the parsed path, in order")
    }

    /** A whole configuration outside the supported subset. */
    private fun unsupported(hexBytes: String, vararg features: AscFeature, declared: DeclaredAudioFacts = stereo48) =
        refused(hexBytes, "a complete configuration", features.toList(), declared)

    /**
     * A prefix that stops at [feature]: the result names only it, and bytes
     * appended after the stop point cannot add a feature, because nothing
     * past it is examined.
     */
    private fun stopsAt(hexBytes: String, feature: AscFeature) {
        refused(hexBytes, "a prefix stopping at ${feature.description}", listOf(feature), stereo48)
        refused(hexBytes + "ffffffff", "the same prefix with bytes after its stop point", listOf(feature), stereo48)
    }

    private fun malformed(hexBytes: String, field: AscField, available: Long, needed: Long) {
        val v = assertIs<AacConfigVerdict.Malformed>(examine(hexBytes), "0x$hexBytes ends before a required field")
        assertEquals(field, v.field, "0x$hexBytes: the field the bytes end before")
        assertEquals(available, v.bitsAvailable, "0x$hexBytes: bits present")
        assertEquals(needed, v.bitsNeeded, "0x$hexBytes: bits that field needed")
    }

    // ---- g: this file's own controls ---------------------------------------

    @Test
    fun `g1 every literal vector is re-derived field by field`() {
        val rows =
            listOf(
                "1190" to bits(*lc()),
                "1188" to bits(*lc(ch = 1)),
                "1194" to bits(*lc(fl = 1)),
                "1210" to bits(*lc(sfi = 4)),
                "1214" to bits(*lc(sfi = 4, fl = 1)),
                "1192" to bits(*lc(dcc = 1)),
                "1191" to bits(*lc(ext = 1)),
                "1180" to bits(*lc(ch = 0)),
                "11b0" to bits(*lc(ch = 6)),
                "11c0" to bits(*lc(ch = 8)),
                "1690" to bits(*lc(sfi = 13)),
                "1710" to bits(*lc(sfi = 14)),
                "0990" to bits(*lc(aot = 1)),
                "299188" to bits(5L to 5, 3L to 4, 2L to 4, 3L to 4, 2L to 5),
                "f990" to bits(*lc(aot = 31)),
                "17805dc010" to bits(2L to 5, 15L to 4, 48_000L to 24, 2L to 4, 0L to 1, 0L to 1, 0L to 1),
                "119056e598" to bits(*lc(), 0x2b7L to 11, 5L to 5, 1L to 1, 3L to 4),
                "119056e500" to bits(*lc(), 0x2b7L to 11, 5L to 5, 0L to 1),
                "119056e5" to bits(*lc(), 0x2b7L to 11, 5L to 5),
                "119056e5f80bb800" to bits(*lc(), 0x2b7L to 11, 5L to 5, 1L to 1, 15L to 4, 96_000L to 24),
                "119056fd" to bits(*lc(), 0x2b7L to 11, 29L to 5),
                "11900000" to bits(*lc(), 0L to 16),
                "119000" to bits(*lc(), 0L to 8),
            )
        for ((literal, derived) in rows) assertContentEquals(derived, hex(literal), "0x$literal")
    }

    @Test
    fun `g2 the multichannel vector is the Microsoft Learn AAC decoder example's AudioSpecificConfig`() {
        // learn.microsoft.com AAC Decoder, "Example Media Types": the last two
        // MF_MT_USER_DATA bytes 0x11 0xb0 decompose as audioObjectType 2,
        // samplingFrequencyIndex 3 (a 48 kHz stream), channelConfiguration 6,
        // then frameLengthFlag, dependsOnCoreCoder and extensionFlag, all 0.
        assertContentEquals(byteArrayOf(0x11, 0xb0.toByte()), bits(*lc(ch = 6)))
    }

    @Test
    fun `g3 declared facts are positive and the substrate snapshots every vector byte for byte`() {
        assertFailsWith<IllegalArgumentException> { DeclaredAudioFacts(0, 2) }
        assertFailsWith<IllegalArgumentException> { DeclaredAudioFacts(48_000, 0) }
        val s = snapshot(hex("119056e598"))
        assertEquals(1, s.unitCount)
        assertContentEquals(hex("119056e598"), s.unit(0))
    }

    // ---- a: the semantics ----------------------------------------------------

    @Test
    fun `a1 AAC-LC 48 kHz stereo and mono with frameLengthFlag 0 are supported with 1024-sample access units`() {
        supported("1190", stereo48, 48_000, 2, 1024)
        supported("1188", mono48, 48_000, 1, 1024)
    }

    @Test
    fun `a2 frameLengthFlag distinguishes configurations whose object type and codec string are identical`() {
        // 0x1190 and 0x1194 differ ONLY in frameLengthFlag; both are object
        // type 2 and both format to mp4a.40.2.
        assertEquals(2, (hex("1190")[0].toInt() and 0xff) ushr 3)
        assertEquals(2, (hex("1194")[0].toInt() and 0xff) ushr 3)
        supported("1190", stereo48, 48_000, 2, 1024)
        unsupported("1194", AscFeature.FrameLengthFlagSet)
    }

    @Test
    fun `a3 the 1210 and 1214 pair is told apart by frameLengthFlag, not by its codec string`() {
        val stereo44 = DeclaredAudioFacts(44_100, 2)
        supported("1210", stereo44, 44_100, 2, 1024)
        unsupported("1214", AscFeature.FrameLengthFlagSet, declared = stereo44)
    }

    @Test
    fun `a3b every indexed frequency is its literal rate, stereo and mono`() {
        val rates = listOf(96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000, 7_350)
        val stereo = listOf("1010", "1090", "1110", "1190", "1210", "1290", "1310", "1390", "1410", "1490", "1510", "1590", "1610")
        val mono = listOf("1008", "1088", "1108", "1188", "1208", "1288", "1308", "1388", "1408", "1488", "1508", "1588", "1608")
        for (i in rates.indices) {
            assertContentEquals(bits(*lc(sfi = i.toLong())), hex(stereo[i]), "setup: index $i stereo vector")
            assertContentEquals(bits(*lc(sfi = i.toLong(), ch = 1)), hex(mono[i]), "setup: index $i mono vector")
            supported(stereo[i], DeclaredAudioFacts(rates[i], 2), rates[i], 2, 1024)
            supported(mono[i], DeclaredAudioFacts(rates[i], 1), rates[i], 1, 1024)
        }
    }

    @Test
    fun `a4 a supported configuration that disagrees with the declared rate or channels is a named mismatch`() {
        for ((declared, rate, channels) in listOf(
            Triple(DeclaredAudioFacts(44_100, 2), true, false),
            Triple(DeclaredAudioFacts(48_000, 1), false, true),
            Triple(DeclaredAudioFacts(44_100, 1), true, true),
            Triple(DeclaredAudioFacts(32_000, 2), true, false),
        )) {
            val v = assertIs<AacConfigVerdict.Mismatched>(examine("1190", declared), "declared ${declared.sampleRate}/${declared.channelCount}")
            assertEquals(48_000, v.actual.sampleRate, "the ASC's own rate is reported")
            assertEquals(2, v.actual.channelCount, "the ASC's own channel count is reported")
            assertEquals(1024, v.actual.framesPerAccessUnit, "the ASC's own access-unit length is reported")
            assertEquals(rate, v.sampleRateDiffers)
            assertEquals(channels, v.channelCountDiffers)
        }
    }

    @Test
    fun `a5 truncation names the field the bytes end before`() {
        malformed("11", AscField.SAMPLING_FREQUENCY_INDEX, 8, 9)
        malformed("1780", AscField.SAMPLING_FREQUENCY, 16, 33)
        malformed("17805dc0", AscField.SAMPLING_FREQUENCY, 32, 33)
        malformed("119056e5", AscField.SBR_PRESENT_FLAG, 32, 33)
        malformed("119056e5f80bb8", AscField.EXTENSION_SAMPLING_FREQUENCY, 56, 61)
    }

    @Test
    fun `a6 other audio object types, explicit SBR and the value 31 stop the examination at the type`() {
        stopsAt("0990", AscFeature.AudioObjectType(1))
        // An explicit-SBR PREFIX: not a complete type-5 configuration.
        stopsAt("299188", AscFeature.AudioObjectType(5))
        // 31 is refused as a prefix, not decoded as a later-edition escape.
        stopsAt("f990", AscFeature.AudioObjectType(31))
    }

    @Test
    fun `a7 an explicit frequency and indices 13 and 14 are outside the subset`() {
        unsupported("17805dc010", AscFeature.ExplicitFrequency(48_000))
        unsupported("1690", AscFeature.FrequencyIndexOutsideSubset(13))
        unsupported("1710", AscFeature.FrequencyIndexOutsideSubset(14))
        // Every feature along the parsed path is reported, in order, not just the first.
        assertContentEquals(bits(*lc(sfi = 13, fl = 1)), hex("1694"), "setup: 0x1694")
        unsupported("1694", AscFeature.FrequencyIndexOutsideSubset(13), AscFeature.FrameLengthFlagSet)
        assertContentEquals(bits(*lc(ch = 6, fl = 1)), hex("11b4"), "setup: 0x11b4")
        unsupported("11b4", AscFeature.ChannelConfiguration(6), AscFeature.FrameLengthFlagSet, declared = DeclaredAudioFacts(48_000, 6))
    }

    @Test
    fun `a8 channel layouts other than 1 or 2, including a program config element, are outside the subset`() {
        // 1180 carries no program config element: a prefix probe.
        stopsAt("1180", AscFeature.ProgramConfigElement)
        unsupported("11b0", AscFeature.ChannelConfiguration(6), declared = DeclaredAudioFacts(48_000, 6))
        stopsAt("11c0", AscFeature.ChannelConfiguration(8))
    }

    @Test
    fun `a9 a core coder dependency and the GA extension flag stop the examination`() {
        // Prefix probes: 1192 carries no 14-bit core-coder delay.
        stopsAt("1192", AscFeature.CoreCoderDependency)
        stopsAt("1191", AscFeature.GaExtensionFlag)
    }

    @Test
    fun `a10 a 0x2b7 extension and any other trailing bits are outside the subset, whatever they signal`() {
        unsupported("119056e598", AscFeature.SyncExtension(5, true))
        unsupported("119056e500", AscFeature.SyncExtension(5, false))
        unsupported("119056fd", AscFeature.SyncExtension(29, null))
        unsupported("11900000", AscFeature.TrailingData(16))
        unsupported("119000", AscFeature.TrailingData(8))
    }

    @Test
    fun `a11 a configuration in more than one unit is outside the subset`() {
        val v = AacConfigSemantics.examine(snapshot(hex("1190"), hex("1190")), stereo48)
        assertEquals(listOf<AscFeature>(AscFeature.UnitCount(2)), assertIs<AacConfigVerdict.Unsupported>(v).features)
    }

    @Test
    fun `a12 diagnostics say unsupported and malformed differently`() {
        val u = assertIs<AacConfigVerdict.Unsupported>(examine("1194"))
        assertEquals("frameLengthFlag 1 (not the 1024-sample access unit)", u.reason)
        val m = assertIs<AacConfigVerdict.Malformed>(examine("11"))
        assertEquals("the configuration ends at bit 8, before its SAMPLING_FREQUENCY_INDEX (which needs 9)", m.reason)
        assertNotEquals<String>(u.reason, m.reason)
    }

    // ---- k: the keying it hands on --------------------------------------------

    private fun expectKeys(facts: AacLcConfigFacts, origin: Long, rate: Int, keys: Map<Long, Long>) {
        val keying = AacConfigSemantics.keyingFor(facts, originUs = origin)
        assertEquals(rate, keying.sampleRate, "the facts' own rate")
        assertEquals(1024, keying.framesPerUnit, "the facts' own access-unit length")
        assertEquals(origin, keying.originUs, "the origin it was given")
        for ((unit, key) in keys) assertEquals(AudioKeyOutcome.Key(key), keying.keyFor(unit), "unit $unit")
    }

    @Test
    fun `k1 keying uses the facts' rate and access-unit length from a nonzero origin`() {
        // 17 + floor(u * 1024 * 1e6 / rate): 44100 -> 23236, 69676; 48000 -> 21350, 64017.
        expectKeys(AacLcConfigFacts(44_100, 2, 1024), 17, 44_100, mapOf(0L to 17, 1L to 23_236, 3L to 69_676))
        expectKeys(AacLcConfigFacts(48_000, 2, 1024), 17, 48_000, mapOf(0L to 17, 1L to 21_350, 3L to 64_017))
    }

    @Test
    fun `k2 an object type 2 configuration that is not 1024-sample yields no facts to key from`() {
        val v = examine("1194")
        assertIs<AacConfigVerdict.Unsupported>(v, "frameLengthFlag 1 must never reach the keying path as 1024")
    }

    @Test
    fun `k3 supported configurations key end to end from their own facts`() {
        val f44 = assertIs<AacConfigVerdict.Supported>(examine("1210", DeclaredAudioFacts(44_100, 2))).facts
        expectKeys(f44, 17, 44_100, mapOf(0L to 17, 1L to 23_236, 3L to 69_676))
        val f48 = assertIs<AacConfigVerdict.Supported>(examine("1190")).facts
        expectKeys(f48, 17, 48_000, mapOf(0L to 17, 1L to 21_350, 3L to 64_017))
    }

    // ---- b: the bound it reads within -------------------------------------------

    @Test
    fun `b1 a long unit within the snapshot bound is classified by its bit count, not refused as oversized`() {
        val unit = hex("1190") + ByteArray(4094)
        val v = AacConfigSemantics.examine(snapshot(unit), stereo48)
        assertEquals(listOf<AscFeature>(AscFeature.TrailingData(4094L * 8)), assertIs<AacConfigVerdict.Unsupported>(v).features)
    }
}
