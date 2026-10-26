package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlinx.coroutines.CancellationException
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.CodecSignaling
import org.openmoq.moq5.CodecSignalingException
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.NativeBridgeInitializationException
import org.openmoq.moq5.SampleEntry

/**
 * The sample's AVC configuration step over an owned [RawConfigSnapshot] of
 * csd-0 and csd-1.
 *
 * `g` rows are this file's own controls. `r` rows measure the real
 * [AvcConfiguration.resolve] against a SCRIPTED [AvcRecordUtility] that logs
 * every call's exact arguments and returns a record the sample cannot
 * construct itself, so a step that formats anything but the returned record
 * is caught. `p` rows go through the PUBLIC binding and its process-wide
 * native owner; this JVM carries no native image, so they observe the owner's
 * refusal. The built record through the real utility is the separate host
 * lane (`host.AvcHostIntegrationTest`), which runs with the image present.
 */
class AvcConfigurationRedTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private fun cat(vararg parts: ByteArray): ByteArray = parts.fold(ByteArray(0)) { a, b -> a + b }

    // Parameter sets: nal_ref_idc 3, types 7 and 8 (ITU-T H.264 Table 7-1).
    private val sc = hex("00000001")
    private val spsA = hex("6742001eabcd")
    private val spsB = hex("6742001fabcd")
    private val ppsA = hex("68ce3c80")
    private val ppsB = hex("68ce3c81")

    private val bounds = ConfigBounds(maxUnits = 4, maxUnitBytes = 1 shl 17, maxTotalBytes = 1 shl 18)

    private fun snapshot(vararg units: ByteArray): RawConfigSnapshot =
        assertIs<RawSnapshotResult.Taken>(RawConfigSnapshots.capture(units.map { ByteBuffer.wrap(it) }, bounds), "setup: the snapshot is taken").snapshot

    /** A record the sample cannot derive from its input: only the utility returns it. */
    private val scriptedRecord = hex("a1b2c3d4e5")

    private class Script(
        private val build: (MediaBytes) -> UtilityOutcome<MediaBytes>,
        private val format: (MediaBytes) -> UtilityOutcome<String>,
    ) : AvcRecordUtility {
        val log = mutableListOf<String>()
        val built = mutableListOf<ByteArray>()
        val formatted = mutableListOf<ByteArray>()

        override fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): UtilityOutcome<MediaBytes> {
            log += "build $source $nalLengthSize"
            built += bytes.bytes()
            return build(bytes)
        }

        override fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes): UtilityOutcome<String> {
            log += "format $format ${sampleEntry.code}"
            formatted += decoderConfig.bytes()
            return format(decoderConfig)
        }
    }

    private fun script() = Script({ UtilityOutcome.Done(MediaBytes(scriptedRecord)) }, { UtilityOutcome.Done("avc1.scripted") })

    private fun resolved(script: Script, vararg units: ByteArray): AvcDecoderConfig =
        assertIs<AvcConfigVerdict.Resolved>(AvcConfiguration.resolve(snapshot(*units), script), "resolved").config

    private fun malformed(defect: AvcDefect, vararg units: ByteArray) {
        val s = script()
        val v = assertIs<AvcConfigVerdict.Malformed>(AvcConfiguration.resolve(snapshot(*units), s), "malformed: ${defect.description}")
        assertEquals(defect, v.defect)
        assertEquals(emptyList(), s.log, "the utility is not called for malformed input")
    }

    private fun outside(refusal: AvcSubsetRefusal, vararg units: ByteArray) {
        val s = script()
        val v = assertIs<AvcConfigVerdict.OutsideSubset>(AvcConfiguration.resolve(snapshot(*units), s), "outside the subset: ${refusal.description}")
        assertEquals(refusal, v.refusal)
        assertEquals(emptyList(), s.log, "the utility is not called for input outside the subset")
    }


    // ---- g: this file's own controls ---------------------------------------

    @Test
    fun `g1 the vectors are carried-forward fixture literals, and the record follows the avcC field layout`() {
        // Provenance, not executable coupling: these literals were copied from
        // the library test fixture CodecVectors.AVC_ANNEXB/.AVCC (exercised
        // through the real utility by CodecBridgeRowsTest c1). Nothing here
        // reads that file.
        assertContentEquals(hex("000000016742001eabcd0000000168ce3c80"), cat(sc, spsA, sc, ppsA))
        // version 1, SPS bytes 1..3, 0xfc|(4-1), 0xe0|1 SPS, 16-bit length,
        // SPS, 1 PPS, 16-bit length, PPS -- derived by hand from the fields.
        val record = cat(hex("01"), spsA.copyOfRange(1, 4), hex("ff"), hex("e1"), hex("0006"), spsA, hex("01"), hex("0004"), ppsA)
        assertContentEquals(hex("0142001effe100066742001eabcd01000468ce3c80"), record)
    }

    @Test
    fun `g2 the production adapter lets the public argument check leave as itself`() {
        // The public precondition runs before the native owner, so this holds
        // with no image. An adapter that caught more than
        // CodecSignalingException would turn it into a Refused outcome.
        // formatCodecString has no argument the adapter can make
        // unrepresentable: its sample entry is validated when constructed and
        // the adapter passes no object type indication.
        val e = assertFailsWith<IllegalArgumentException> {
            PublicCodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(sc + spsA), 256)
        }
        assertEquals(false, e is CodecSignalingException, "an argument check, not a native refusal")
    }

    // ---- r: the configuration step --------------------------------------------

    @Test
    fun `r1 one SPS and one PPS build once and format exactly the returned record`() {
        val s = script()
        val c = resolved(s, cat(sc, spsA), cat(sc, ppsA))
        assertEquals(listOf("build AvcAnnexB 4", "format AvcRecord avc1"), s.log, "exactly one build then one format")
        assertContentEquals(cat(sc, spsA, sc, ppsA), s.built.single(), "the parameter sets, bytes and order exact")
        assertContentEquals(scriptedRecord, s.formatted.single(), "the record the utility returned is what is formatted")
        assertContentEquals(scriptedRecord, c.record.bytes())
        assertEquals("avc1.scripted", c.codecString)
        assertEquals(1, c.sequenceParameterSets)
        assertEquals(1, c.pictureParameterSets)
    }

    @Test
    fun `r2 several parameter sets keep their declared order`() {
        val s = script()
        val c = resolved(s, cat(sc, spsA, sc, spsB), cat(sc, ppsA, sc, ppsB))
        assertContentEquals(cat(sc, spsA, sc, spsB, sc, ppsA, sc, ppsB), s.built.single())
        assertEquals(2, c.sequenceParameterSets)
        assertEquals(2, c.pictureParameterSets)
    }

    @Test
    fun `r3 a unit boundary is never repaired by the next unit`() {
        // csd-0 ends 00 00 and csd-1 begins 01: concatenated, they would read
        // as a THREE-byte prefix.
        outside(AvcSubsetRefusal.TrailingZeroBytes(0, 0), cat(sc, spsA, hex("0000")), cat(hex("01"), ppsA))
        // csd-0 ends 00 00 00 and csd-1 begins 01: concatenated, a clean
        // four-byte start code. Each unit is refused on its own.
        outside(AvcSubsetRefusal.TrailingZeroBytes(0, 0), cat(sc, spsA, hex("000000")), cat(hex("01"), ppsA))
        malformed(AvcDefect.MissingStartCode(1), cat(sc, spsA), cat(hex("01"), ppsA))
        malformed(AvcDefect.ThreeByteStartCode(1, 0), cat(sc, spsA), cat(hex("000001"), ppsA))
    }

    @Test
    fun `r4 exactly two units, csd-0 and csd-1`() {
        outside(AvcSubsetRefusal.UnitCount(1), cat(sc, spsA, sc, ppsA))
        outside(AvcSubsetRefusal.UnitCount(3), cat(sc, spsA), cat(sc, ppsA), cat(sc, ppsB))
    }

    @Test
    fun `r5 each unit begins with its start code, not with other bytes`() {
        malformed(AvcDefect.MissingStartCode(0), spsA, cat(sc, ppsA))
        malformed(AvcDefect.MissingStartCode(0), cat(hex("ff"), sc, spsA), cat(sc, ppsA))
        outside(AvcSubsetRefusal.LeadingZeroBytes(0), cat(hex("00"), sc, spsA), cat(sc, ppsA))
    }

    @Test
    fun `r6 a parameter set needs the four-byte start code`() {
        malformed(AvcDefect.ThreeByteStartCode(0, 0), cat(hex("000001"), spsA), cat(sc, ppsA))
        malformed(AvcDefect.ThreeByteStartCode(0, 10), cat(sc, spsA, hex("000001"), spsB), cat(sc, ppsA))
    }

    @Test
    fun `r7 an empty NAL unit is malformed`() {
        malformed(AvcDefect.EmptyNal(0, 0), cat(sc, sc, spsA), cat(sc, ppsA))
        malformed(AvcDefect.EmptyNal(1, 8), cat(sc, spsA), cat(sc, ppsA, sc))
    }

    @Test
    fun `r8 forbidden_zero_bit and a parameter set's nal_ref_idc are checked`() {
        malformed(AvcDefect.ForbiddenBitSet(0, 0), cat(sc, hex("e742001eabcd")), cat(sc, ppsA))
        malformed(AvcDefect.ZeroReferenceIndication(0, 0, 7), cat(sc, hex("0742001eabcd")), cat(sc, ppsA))
        malformed(AvcDefect.ZeroReferenceIndication(1, 0, 8), cat(sc, spsA), cat(sc, hex("08ce3c80")))
    }

    @Test
    fun `r9 non-parameter-set NAL units are refused, never ignored`() {
        outside(AvcSubsetRefusal.NalUnitType(0, 10, 9), cat(sc, spsA, sc, hex("09f0")), cat(sc, ppsA))
        outside(AvcSubsetRefusal.NalUnitType(0, 0, 6), cat(sc, hex("0605ff80")), cat(sc, ppsA))
        outside(AvcSubsetRefusal.NalUnitType(0, 0, 5), cat(sc, hex("6588")), cat(sc, ppsA))
        outside(AvcSubsetRefusal.NalUnitType(1, 0, 14), cat(sc, spsA), cat(sc, hex("6e80")))
        outside(AvcSubsetRefusal.NalUnitType(1, 0, 0), cat(sc, spsA), cat(sc, hex("6080")))
    }

    @Test
    fun `r10 SPS belong in csd-0 and PPS in csd-1`() {
        outside(AvcSubsetRefusal.MisplacedParameterSet(0, 10, 8), cat(sc, spsA, sc, ppsA), cat(sc, ppsB))
        outside(AvcSubsetRefusal.MisplacedParameterSet(1, 0, 7), cat(sc, spsA), cat(sc, spsB, sc, ppsA))
    }

    @Test
    fun `r11 a byte-identical parameter set is refused, not deduplicated`() {
        outside(AvcSubsetRefusal.DuplicateParameterSet(0, 10), cat(sc, spsA, sc, spsA), cat(sc, ppsA))
        outside(AvcSubsetRefusal.DuplicateParameterSet(1, 8), cat(sc, spsA), cat(sc, ppsA, sc, ppsA))
    }

    @Test
    fun `r12 the utility's parameter-set count limit, for each type, refused on observing the 17th`() {
        fun sps(i: Int) = cat(sc, hex("6742001e"), byteArrayOf((i + 1).toByte()), hex("cd"))
        fun pps(i: Int) = cat(sc, hex("68ce"), byteArrayOf((i + 1).toByte()), hex("80"))
        val sixteenSps = cat(*Array(16) { sps(it) })
        val sixteenPps = cat(*Array(16) { pps(it) })
        val s = script()
        assertEquals(16, resolved(s, sixteenSps, cat(sc, ppsA)).sequenceParameterSets, "16 SPS are within the limit")
        assertContentEquals(cat(sixteenSps, sc, ppsA), s.built.single(), "all 16 SPS reach the utility, bytes exact")
        val t = script()
        assertEquals(16, resolved(t, cat(sc, spsA), sixteenPps).pictureParameterSets, "16 PPS are within the limit")
        assertContentEquals(cat(sc, spsA, sixteenPps), t.built.single(), "all 16 PPS reach the utility, bytes exact")
        outside(AvcSubsetRefusal.TooManyParameterSets(7, 17, 16), cat(sixteenSps, sps(16)), cat(sc, ppsA))
        outside(AvcSubsetRefusal.TooManyParameterSets(8, 17, 16), cat(sc, spsA), cat(sixteenPps, pps(16)))
        // Refused on OBSERVING the 17th: a defect after it is never reached.
        outside(AvcSubsetRefusal.TooManyParameterSets(7, 17, 16), cat(sixteenSps, sps(16), sc, hex("e742001eabcd")), cat(sc, ppsA))
    }

    @Test
    fun `r13 a parameter set longer than a 16-bit length is refused before the utility, for each type`() {
        val longestSps = cat(hex("6742001e"), ByteArray(0xffff - 4) { 0xab.toByte() })
        val longestPps = cat(hex("68"), ByteArray(0xffff - 1) { 0xab.toByte() })
        assertEquals(0xffff, longestSps.size, "setup")
        assertEquals(0xffff, longestPps.size, "setup")
        val s = script()
        resolved(s, cat(sc, longestSps), cat(sc, ppsA))
        assertContentEquals(cat(sc, longestSps, sc, ppsA), s.built.single(), "the 65535-byte SPS reaches the utility, every byte")
        val t = script()
        resolved(t, cat(sc, spsA), cat(sc, longestPps))
        assertContentEquals(cat(sc, spsA, sc, longestPps), t.built.single(), "the 65535-byte PPS reaches the utility, every byte")
        outside(AvcSubsetRefusal.ParameterSetTooLong(0, 0, 0x10000, 0xffff), cat(sc, longestSps, hex("ab")), cat(sc, ppsA))
        outside(AvcSubsetRefusal.ParameterSetTooLong(1, 0, 0x10000, 0xffff), cat(sc, spsA), cat(sc, longestPps, hex("ab")))
    }

    @Test
    fun `r14 zero bytes after a NAL unit are refused`() {
        outside(AvcSubsetRefusal.TrailingZeroBytes(1, 0), cat(sc, spsA), cat(sc, ppsA, hex("00")))
        outside(AvcSubsetRefusal.TrailingZeroBytes(0, 0), cat(sc, spsA, hex("00"), sc, spsB), cat(sc, ppsA))
    }

    @Test
    fun `r15 emulation-prevention violations are malformed in either unit`() {
        // The forbidden three-byte sequences (checked first at a position)...
        malformed(AvcDefect.EmulationViolation(0, 0), cat(sc, hex("67420000001ecd")), cat(sc, ppsA))
        malformed(AvcDefect.EmulationViolation(0, 0), cat(sc, hex("67420000021ecd")), cat(sc, ppsA))
        malformed(AvcDefect.EmulationViolation(1, 0), cat(sc, spsA), cat(sc, hex("68ce0000003c80")))
        // ...and 00 00 03 followed by anything but 00, 01, 02 or 03.
        for (follower in listOf("04", "ff")) {
            malformed(AvcDefect.EmulationViolation(0, 0), cat(sc, hex("6742000003${follower}1ecd")), cat(sc, ppsA))
            malformed(AvcDefect.EmulationViolation(1, 0), cat(sc, spsA), cat(sc, hex("68ce000003${follower}3c80")))
        }
    }

    @Test
    fun `r15b the four permitted escapes, and a terminal 00 00 03, pass through unchanged in either unit`() {
        for (follower in listOf("00", "01", "02", "03")) {
            val sps = hex("6742000003${follower}1ecd")
            val pps = hex("68ce000003${follower}3c80")
            val s = script()
            resolved(s, cat(sc, sps), cat(sc, pps))
            assertContentEquals(cat(sc, sps, sc, pps), s.built.single(), "00 00 03 $follower is kept, byte for byte")
        }
        // The four-byte rule constrains four-byte sequences only; a NAL that
        // ends 00 00 03 is not refused by it (the sample validates no RBSP).
        val terminal = hex("6742001e000003")
        val s = script()
        resolved(s, cat(sc, terminal), cat(sc, ppsA))
        assertContentEquals(cat(sc, terminal, sc, ppsA), s.built.single(), "a terminal 00 00 03 is kept")
    }

    @Test
    fun `r16 a utility refusal is a verdict at its stage with its code`() {
        val proto = MoqResultCode(-3)
        val inval = MoqResultCode(-2)
        val b = Script({ UtilityOutcome.Refused(proto) }, { UtilityOutcome.Done("unused") })
        val vb = assertIs<AvcConfigVerdict.UtilityRefused>(AvcConfiguration.resolve(snapshot(cat(sc, spsA), cat(sc, ppsA)), b))
        assertEquals(UtilityStage.BUILD, vb.stage)
        assertEquals(proto, vb.code)
        assertEquals(listOf("build AvcAnnexB 4"), b.log, "no format after a refused build")
        val f = Script({ UtilityOutcome.Done(MediaBytes(scriptedRecord)) }, { UtilityOutcome.Refused(inval) })
        val vf = assertIs<AvcConfigVerdict.UtilityRefused>(AvcConfiguration.resolve(snapshot(cat(sc, spsA), cat(sc, ppsA)), f))
        assertEquals(UtilityStage.FORMAT, vf.stage)
        assertEquals(inval, vf.code)
    }

    @Test
    fun `r17 anything but a utility refusal leaves as itself`() {
        val input = { snapshot(cat(sc, spsA), cat(sc, ppsA)) }
        for (t in listOf<Throwable>(
            IllegalStateException("contract failure"),
            IllegalArgumentException("not a codec refusal"),
            OutOfMemoryError("allocation"),
            CancellationException("cancelled"),
        )) {
            val thrown = assertFailsWith<Throwable> { AvcConfiguration.resolve(input(), Script({ throw t }, { UtilityOutcome.Done("unused") })) }
            assertSame(t, thrown, "${t.javaClass.simpleName} from the build")
            val thrownFormat = assertFailsWith<Throwable> { AvcConfiguration.resolve(input(), Script({ UtilityOutcome.Done(MediaBytes(scriptedRecord)) }, { throw t })) }
            assertSame(t, thrownFormat, "${t.javaClass.simpleName} from the format")
        }
    }

    @Test
    fun `r18 the owned snapshot is unchanged by resolution`() {
        val snap = snapshot(cat(sc, spsA), cat(sc, ppsA))
        val before = snapshot(cat(sc, spsA), cat(sc, ppsA))
        AvcConfiguration.resolve(snap, script())
        assertEquals(before, snap, "raw equality is opaque and unchanged")
    }

    @Test
    fun `r19 the resolved configuration carries every validated set, in declared order, as its own copies`() {
        val csd0 = cat(sc, spsA, sc, spsB)
        val csd1 = cat(sc, ppsA, sc, ppsB)
        val c = resolved(script(), csd0, csd1)
        val sets = c.parameterSets
        assertEquals(2, sets.sequenceParameterSets)
        assertEquals(2, sets.pictureParameterSets)
        assertContentEquals(spsA, sets.sequenceParameterSet(0))
        assertContentEquals(spsB, sets.sequenceParameterSet(1))
        assertContentEquals(ppsA, sets.pictureParameterSet(0))
        assertContentEquals(ppsB, sets.pictureParameterSet(1))
        assertEquals(c.sequenceParameterSets, sets.sequenceParameterSets, "the count fields agree with the inventory")
        assertEquals(c.pictureParameterSets, sets.pictureParameterSets)
        // Neither the source arrays nor an accessor array reach the inventory.
        csd0.fill(0x11)
        csd1.fill(0x11)
        sets.sequenceParameterSet(1)[1] = 0x00
        sets.pictureParameterSet(1)[1] = 0x00
        assertContentEquals(spsB, c.parameterSets.sequenceParameterSet(1))
        assertContentEquals(ppsB, c.parameterSets.pictureParameterSet(1))
        assertEquals(true, c.parameterSets.matches(7, spsB, 0, spsB.size), "a non-first SPS is authority")
        assertEquals(true, c.parameterSets.matches(8, ppsB, 0, ppsB.size), "a non-first PPS is authority")
    }

    @Test
    fun `r20 the inventory is the validated input, not the returned record, at both resolver limits`() {
        // The scripted record shares no bytes with the sets: the inventory comes from csd-0 and csd-1.
        val one = resolved(script(), cat(sc, spsA), cat(sc, ppsA)).parameterSets
        assertContentEquals(spsA, one.sequenceParameterSet(0))
        assertContentEquals(ppsA, one.pictureParameterSet(0))
        fun sps(i: Int) = cat(hex("6742001e"), byteArrayOf((i + 1).toByte()), hex("cd"))
        val sixteen = resolved(script(), cat(*Array(16) { cat(sc, sps(it)) }), cat(sc, ppsA)).parameterSets
        assertEquals(16, sixteen.sequenceParameterSets)
        for (i in 0 until 16) assertContentEquals(sps(i), sixteen.sequenceParameterSet(i), "SPS $i")
        val longestPps = cat(hex("68"), ByteArray(0xffff - 1) { 0xab.toByte() })
        val longest = resolved(script(), cat(sc, spsA), cat(sc, longestPps)).parameterSets
        assertContentEquals(longestPps, longest.pictureParameterSet(0), "the 65535-byte PPS, every byte")
    }

    @Test
    fun `r21 a frame converts against the resolved configuration's own inventory`() {
        val c = resolved(script(), cat(sc, spsA, sc, spsB), cat(sc, ppsA, sc, ppsB))
        val idr = hex("65888421")
        val v = assertIs<AvcPayloadVerdict.Converted>(
            AvcPayloads.convert(MediaBytes(cat(sc, spsB, sc, ppsB, sc, idr)), c.parameterSets, AvcPayloadLimits(16, 1 shl 16)),
        )
        assertContentEquals(hex("0000000465888421"), v.payload.bytes())
        assertEquals(1, v.strippedSequenceParameterSets)
        assertEquals(1, v.strippedPictureParameterSets)
        assertEquals(10L, v.strippedParameterSetBytes)
        val unknown = AvcPayloads.convert(MediaBytes(cat(sc, hex("6742001dabcd"), sc, idr)), c.parameterSets, AvcPayloadLimits(16, 1 shl 16))
        assertEquals(AvcPayloadRefusal.UnknownParameterSet(0, 7), assertIs<AvcPayloadVerdict.OutsideSubset>(unknown).refusal)
    }

    // ---- p: the public binding path ---------------------------------------------

    @Test
    fun `p1 through the public binding, an unavailable native owner leaves as itself, not as a verdict`() {
        assertFailsWith<NativeBridgeInitializationException> {
            AvcConfiguration.resolve(snapshot(cat(sc, spsA), cat(sc, ppsA)))
        }
    }

    @Test
    fun `p2 each adapter method lets the owner refusal leave as itself, not as a Refused outcome`() {
        // resolve never reaches the format step here (the build meets the
        // owner first), so each adapter method is called directly.
        assertFailsWith<NativeBridgeInitializationException> {
            PublicCodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(cat(sc, spsA, sc, ppsA)), 4)
        }
        assertFailsWith<NativeBridgeInitializationException> {
            PublicCodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), MediaBytes(scriptedRecord))
        }
    }
}
