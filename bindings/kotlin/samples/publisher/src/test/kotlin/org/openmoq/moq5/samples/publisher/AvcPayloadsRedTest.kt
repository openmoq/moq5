package org.openmoq.moq5.samples.publisher

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.UnixMicros

/**
 * The sample's AVC payload conversion: a frame's Annex-B bytes to the avc1
 * payload of four-byte big-endian lengths, stripping only parameter sets
 * byte-identical to the resolved configuration's.
 *
 * Every vector is a literal laid out by hand. A NAL header byte is
 * `nal_ref_idc << 5 | nal_unit_type` (ITU-T H.264 7.3.1), an expected length
 * is its four big-endian bytes, and an offset is the byte position of a NAL
 * unit's start code in the frame. The IDR and non-IDR slice bytes are
 * FRAMING vectors, not a valid coded picture. `g` rows are this file's own
 * controls; `c` rows measure framing and bounds, `p` rows parameter-set
 * policy, of the real [AvcPayloads.convert]. Every conversion row observes
 * the output allocator: exactly one request of the exact size on success,
 * none on any refusal.
 */
class AvcPayloadsRedTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private fun cat(vararg parts: ByteArray): ByteArray = parts.fold(ByteArray(0)) { a, b -> a + b }

    private val sc4 = hex("00000001")
    private val sc3 = hex("000001")

    // nal_ref_idc/type: 3/5 IDR, 0/9 AUD, 0/6 SEI, 2/1 non-IDR slice, 3/7 SPS, 3/8 PPS
    private val idr = hex("65888421")
    private val aud = hex("09f0")
    private val sei = hex("060501ab80")
    private val slice = hex("419a02")
    private val spsA = hex("6742001eabcd")
    private val ppsA = hex("68ce3c80")

    // Second sets of each type, for an inventory whose match is not its first entry.
    private val spsB = hex("6742001fabcd")
    private val ppsB = hex("68ee3c80")

    private val inventory = AvcParameterSetInventory.copyOf(listOf(spsA), listOf(ppsA))
    private val roomy = AvcPayloadLimits(maxNalUnits = 16, maxOutputBytes = 1 shl 16)

    /** Records every output request; optionally fails, or answers [sizeDelta] bytes off the request. */
    private class Counting(private val failure: Throwable? = null, private val sizeDelta: Int = 0) : AvcOutputAllocator {
        val requests = mutableListOf<Int>()

        override fun allocate(size: Int): ByteArray {
            requests += size
            failure?.let { throw it }
            return ByteArray(size + sizeDelta)
        }
    }

    private fun converted(
        frame: ByteArray,
        expected: ByteArray,
        types: List<Int>,
        limits: AvcPayloadLimits = roomy,
        strippedSps: Int = 0,
        strippedPps: Int = 0,
        strippedBytes: Long = 0,
        config: AvcParameterSetInventory = inventory,
    ) {
        val a = Counting()
        val v = assertIs<AvcPayloadVerdict.Converted>(AvcPayloads.convert(MediaBytes(frame), config, limits, a), "converted")
        assertContentEquals(expected, v.payload.bytes(), "the avc1 payload, byte for byte")
        assertEquals(types, v.nalUnitTypes, "retained NAL unit types, in order")
        assertEquals(strippedSps, v.strippedSequenceParameterSets, "stripped SPS occurrences")
        assertEquals(strippedPps, v.strippedPictureParameterSets, "stripped PPS occurrences")
        assertEquals(strippedBytes, v.strippedParameterSetBytes, "stripped NAL bytes, start codes excluded")
        assertEquals(listOf(expected.size), a.requests, "exactly one output request, of the exact size")
    }

    private fun malformed(frame: ByteArray, defect: AvcPayloadDefect) {
        val a = Counting()
        val v = assertIs<AvcPayloadVerdict.Malformed>(AvcPayloads.convert(MediaBytes(frame), inventory, roomy, a), "malformed: ${defect.description}")
        assertEquals(defect, v.defect)
        assertEquals(emptyList(), a.requests, "no output requested for malformed input")
    }

    private fun outside(
        frame: ByteArray,
        refusal: AvcPayloadRefusal,
        limits: AvcPayloadLimits = roomy,
        config: AvcParameterSetInventory = inventory,
    ) {
        val a = Counting()
        val v = assertIs<AvcPayloadVerdict.OutsideSubset>(AvcPayloads.convert(MediaBytes(frame), config, limits, a), "outside: ${refusal.description}")
        assertEquals(refusal, v.refusal)
        assertEquals(emptyList(), a.requests, "no output requested for a refused frame")
    }

    // ---- g: this file's own controls ---------------------------------------

    @Test
    fun `g1 the header bytes are the fields they claim`() {
        fun header(ref: Int, type: Int) = ((ref shl 5) or type).toByte()
        assertEquals(header(3, 5), idr[0])
        assertEquals(header(0, 9), aud[0])
        assertEquals(header(0, 6), sei[0])
        assertEquals(header(2, 1), slice[0])
        assertEquals(header(3, 7), spsA[0])
        assertEquals(header(3, 8), ppsA[0])
    }

    @Test
    fun `g2 limits are positive and an inventory holds at least one non-empty set of each type`() {
        assertFailsWith<IllegalArgumentException> { AvcPayloadLimits(0, 1) }
        assertFailsWith<IllegalArgumentException> { AvcPayloadLimits(1, 0) }
        assertFailsWith<IllegalArgumentException> { AvcParameterSetInventory.copyOf(emptyList(), listOf(ppsA)) }
        assertFailsWith<IllegalArgumentException> { AvcParameterSetInventory.copyOf(listOf(spsA), emptyList()) }
        assertFailsWith<IllegalArgumentException> { AvcParameterSetInventory.copyOf(listOf(ByteArray(0)), listOf(ppsA)) }
    }

    @Test
    fun `g3 flag classification by EncodedOutputs - evidence about decide, not about routing into the converter`() {
        val timeline = PublishTimeline(0)
        val capture = UnixMicros(1_000_000uL)
        fun decide(bytes: ByteArray, flags: Int, at: UnixMicros? = null) =
            EncodedOutputs.decide(EncodedOutput(bytes, 0, bytes.size, 0, flags), MediaKind.Video, timeline, at)
        assertIs<OutputDecision.Config>(decide(cat(sc4, idr), CaptureContract.FLAG_CODEC_CONFIG))
        assertIs<OutputDecision.EndOfStream>(decide(ByteArray(0), CaptureContract.FLAG_END_OF_STREAM))
        assertIs<OutputDecision.Rejected>(decide(ByteArray(0), 0), "an empty non-terminal output")
        assertIs<OutputDecision.Rejected>(decide(cat(sc4, idr), CaptureContract.FLAG_PARTIAL_FRAME))
        assertIs<OutputDecision.Rejected>(
            decide(cat(sc4, idr), CaptureContract.FLAG_PARTIAL_FRAME or CaptureContract.FLAG_CODEC_CONFIG),
            "partial takes precedence over codec config",
        )
        // A NON-empty terminal output is media: it becomes a frame, with its exact payload.
        val terminal = assertIs<OutputDecision.Frame>(decide(cat(sc4, idr), CaptureContract.FLAG_END_OF_STREAM, capture))
        assertTrue(terminal.endsStream)
        assertContentEquals(cat(sc4, idr), terminal.obj.payload.bytes())
    }

    @Test
    fun `g4 an inventory holds 1 to 16 sets of each type, each 1 to 65535 bytes, checked independently per type`() {
        val max = AvcConfiguration.MAX_PARAMETER_SETS
        val longest = AvcConfiguration.MAX_PARAMETER_SET_BYTES
        assertEquals(16, max)
        assertEquals(0xffff, longest)
        // Distinct sets of each type: the header byte, then the index.
        fun sets(type: Int, n: Int) = List(n) { byteArrayOf((0x60 or type).toByte(), (it + 1).toByte()) }
        fun long(type: Int, n: Int) = ByteArray(n) { if (it == 0) (0x60 or type).toByte() else 0xab.toByte() }

        val full = AvcParameterSetInventory.copyOf(sets(7, max), sets(8, max))
        assertEquals(max, full.sequenceParameterSets)
        assertEquals(max, full.pictureParameterSets)
        assertFailsWith<IllegalArgumentException>("17 SPS") { AvcParameterSetInventory.copyOf(sets(7, max + 1), listOf(ppsA)) }
        assertFailsWith<IllegalArgumentException>("17 PPS") { AvcParameterSetInventory.copyOf(listOf(spsA), sets(8, max + 1)) }

        val edge = AvcParameterSetInventory.copyOf(listOf(long(7, longest)), listOf(long(8, longest)))
        assertEquals(longest, edge.sequenceParameterSet(0).size)
        assertEquals(longest, edge.pictureParameterSet(0).size)
        assertFailsWith<IllegalArgumentException>("a 65536-byte SPS") { AvcParameterSetInventory.copyOf(listOf(long(7, longest + 1)), listOf(ppsA)) }
        assertFailsWith<IllegalArgumentException>("a 65536-byte PPS") { AvcParameterSetInventory.copyOf(listOf(spsA), listOf(long(8, longest + 1))) }
        // A bad set that is not first is still seen.
        assertFailsWith<IllegalArgumentException>("a later over-long SPS") { AvcParameterSetInventory.copyOf(listOf(spsA, long(7, longest + 1)), listOf(ppsA)) }
        assertFailsWith<IllegalArgumentException>("a later over-long PPS") { AvcParameterSetInventory.copyOf(listOf(spsA), listOf(ppsA, long(8, longest + 1))) }
        assertFailsWith<IllegalArgumentException>("an empty PPS") { AvcParameterSetInventory.copyOf(listOf(spsA), listOf(ppsA, ByteArray(0))) }
    }

    @Test
    fun `g5 the inventory owns its sets - no caller list, caller array or accessor array reaches it`() {
        val spsList = mutableListOf(spsA.copyOf(), spsB.copyOf())
        val ppsList = mutableListOf(ppsA.copyOf(), ppsB.copyOf())
        val owned = AvcParameterSetInventory.copyOf(spsList, ppsList)
        spsList[1][1] = 0x00
        ppsList[1][1] = 0x00
        spsList.clear()
        ppsList.add(ppsA)
        owned.sequenceParameterSet(1)[2] = 0x7f
        owned.pictureParameterSet(1)[2] = 0x7f
        assertEquals(2, owned.sequenceParameterSets)
        assertEquals(2, owned.pictureParameterSets)
        assertContentEquals(spsA, owned.sequenceParameterSet(0))
        assertContentEquals(spsB, owned.sequenceParameterSet(1))
        assertContentEquals(ppsA, owned.pictureParameterSet(0))
        assertContentEquals(ppsB, owned.pictureParameterSet(1))
        assertNotSame(owned.pictureParameterSet(1), owned.pictureParameterSet(1), "a fresh array per call")
        assertTrue(owned.matches(7, spsB, 0, spsB.size))
        assertTrue(owned.matches(8, ppsB, 0, ppsB.size))
    }

    @Test
    fun `g6 converted NAL types are owned - neither the constructor input nor an accessor list reaches them`() {
        val input = mutableListOf(9, 5, 1)
        val v = AvcPayloadVerdict.Converted(MediaBytes(hex("00")), input, 0, 0, 0)
        input.clear()
        assertEquals(listOf(9, 5, 1), v.nalUnitTypes, "the constructor input was copied")
        val seen = v.nalUnitTypes
        runCatching { (seen as MutableList<Int>).add(6) }
        assertEquals(listOf(9, 5, 1), v.nalUnitTypes, "an accessor list is not the holder's state")
        assertNotSame(v.nalUnitTypes, v.nalUnitTypes, "a fresh list per call")
    }

    // ---- c: framing and bounds ----------------------------------------------------

    @Test
    fun `c1 one IDR slice gets its big-endian four-byte length`() {
        converted(cat(sc4, idr), hex("0000000465888421"), listOf(5))
    }

    @Test
    fun `c2 several NAL units keep their order and bytes, from four- and three-byte start codes`() {
        val frame = cat(sc4, aud, sc3, sei, sc4, idr, sc3, slice)
        val expected = hex("0000000209f0" + "00000005060501ab80" + "0000000465888421" + "00000003419a02")
        converted(frame, expected, listOf(9, 6, 5, 1))
        assertEquals(28, frame.size)
        assertEquals(30, expected.size, "two three-byte start codes make the output two bytes LONGER than the input")
    }

    @Test
    fun `c3 the output bound is exact and nothing is requested from the allocator over it`() {
        val frame = cat(sc4, aud, sc3, sei, sc4, idr, sc3, slice)
        val expected = hex("0000000209f000000005060501ab800000000465888421" + "00000003419a02")
        converted(frame, expected, listOf(9, 6, 5, 1), AvcPayloadLimits(16, 30))
        outside(frame, AvcPayloadRefusal.OutputTooLarge(30, 29), AvcPayloadLimits(16, 29))
    }

    @Test
    fun `c4 a frame begins exactly with a four-byte start code`() {
        malformed(cat(sc3, idr), AvcPayloadDefect.ThreeByteAccessUnitStart)
        malformed(idr, AvcPayloadDefect.MissingStartCode)
        malformed(cat(hex("ff"), sc4, idr), AvcPayloadDefect.MissingStartCode)
        outside(cat(hex("00"), sc4, idr), AvcPayloadRefusal.LeadingZeroBytes)
    }

    @Test
    fun `c5 an empty NAL unit is malformed`() {
        malformed(cat(sc4, sc4, idr), AvcPayloadDefect.EmptyNal(0))
        malformed(cat(sc4, idr, sc4), AvcPayloadDefect.EmptyNal(8))
    }

    @Test
    fun `c6 zero bytes after a NAL unit are refused, never dropped`() {
        outside(cat(sc4, idr, hex("00")), AvcPayloadRefusal.TrailingZeroBytes(0))
        outside(cat(sc4, aud, hex("00"), sc4, idr), AvcPayloadRefusal.TrailingZeroBytes(0))
        // One zero before a three-byte prefix is that start code's zero_byte, not trailing data.
        converted(cat(sc4, aud, hex("00"), sc3, idr), hex("0000000209f0" + "0000000465888421"), listOf(9, 5))
    }

    @Test
    fun `c7 all four legal escape successors are kept and every other successor is malformed`() {
        for (follower in listOf("00", "01", "02", "03")) {
            val nal = hex("6588000003${follower}21")
            converted(cat(sc4, nal), cat(hex("00000007"), nal), listOf(5))
        }
        for (follower in listOf("04", "7f", "80", "ff")) {
            malformed(cat(sc4, hex("6588000003${follower}21")), AvcPayloadDefect.EmulationViolation(0))
        }
        malformed(cat(sc4, hex("658800000021")), AvcPayloadDefect.EmulationViolation(0))
        malformed(cat(sc4, hex("658800000221")), AvcPayloadDefect.EmulationViolation(0))
    }

    @Test
    fun `c8 NAL unit types neither converted nor parameter sets are refused, including reserved and unspecified`() {
        for (type in listOf(2, 3, 4, 10, 11, 12, 13, 14, 20, 23, 0, 24, 31)) {
            outside(cat(sc4, byteArrayOf((0x60 or type).toByte(), 0x80.toByte())), AvcPayloadRefusal.NalUnitType(0, type))
        }
    }

    @Test
    fun `c9 nal_ref_idc must be non-zero on IDR and zero on SEI and AUD`() {
        malformed(cat(sc4, hex("05888421")), AvcPayloadDefect.ReferenceIndication(0, 5, 0))
        malformed(cat(sc4, hex("660501ab80")), AvcPayloadDefect.ReferenceIndication(0, 6, 3))
        malformed(cat(sc4, hex("29f0")), AvcPayloadDefect.ReferenceIndication(0, 9, 1))
    }

    @Test
    fun `c10 a one-byte NAL unit is refused - its length 1 would read as a start code`() {
        outside(cat(sc4, hex("21")), AvcPayloadRefusal.LengthOneAmbiguous(0))
    }

    @Test
    fun `c11 the NAL bound counts every NAL unit and refuses on observing the first one over`() {
        val three = AvcPayloadLimits(3, 1 shl 16)
        converted(cat(sc4, aud, sc4, sei, sc4, idr), hex("0000000209f000000005060501ab800000000465888421"), listOf(9, 6, 5), three)
        outside(cat(sc4, aud, sc4, sei, sc4, idr, sc4, slice), AvcPayloadRefusal.TooManyNalUnits(4, 3), three)
        // A defect AFTER the first unit over the bound is never reached.
        outside(cat(sc4, aud, sc4, sei, sc4, idr, sc4, slice, sc4, hex("e5888421")), AvcPayloadRefusal.TooManyNalUnits(4, 3), three)
        // Stripped parameter sets count too.
        converted(cat(sc4, spsA, sc4, ppsA, sc4, idr), hex("0000000465888421"), listOf(5), three, 1, 1, 10)
        outside(cat(sc4, spsA, sc4, ppsA, sc4, idr, sc4, slice), AvcPayloadRefusal.TooManyNalUnits(4, 3), three)
    }

    @Test
    fun `c12 a length above one byte is big-endian`() {
        val big = cat(hex("65"), ByteArray(299) { 0xab.toByte() })
        converted(cat(sc4, big), cat(hex("0000012c"), big), listOf(5))
    }

    @Test
    fun `c13 forbidden_zero_bit is malformed`() {
        malformed(cat(sc4, hex("e5888421")), AvcPayloadDefect.ForbiddenBitSet(0))
    }

    @Test
    fun `c14 the input frame is unchanged by conversion`() {
        val frame = MediaBytes(cat(sc4, aud, sc3, idr))
        val before = frame.bytes()
        val a = Counting()
        val v = assertIs<AvcPayloadVerdict.Converted>(AvcPayloads.convert(frame, inventory, roomy, a))
        assertContentEquals(hex("0000000209f0" + "0000000465888421"), v.payload.bytes())
        assertEquals(listOf(14), a.requests)
        assertContentEquals(before, frame.bytes())
    }

    @Test
    fun `c15 an unbounded NAL limit allocates nothing in proportion to it`() {
        converted(cat(sc4, idr), hex("0000000465888421"), listOf(5), AvcPayloadLimits(Int.MAX_VALUE, 1 shl 16))
    }

    // ---- p: in-band parameter sets ------------------------------------------------

    @Test
    fun `p1 identical SPS and PPS are stripped and counted, the slice kept exactly`() {
        converted(cat(sc4, spsA, sc4, ppsA, sc4, idr), hex("0000000465888421"), listOf(5), strippedSps = 1, strippedPps = 1, strippedBytes = 10)
    }

    @Test
    fun `p2 repeated identical sets among mixed prefixes are all stripped, the rest kept in order`() {
        val frame = cat(sc4, aud, sc4, spsA, sc4, ppsA, sc3, sei, sc4, spsA, sc4, ppsA, sc4, idr, sc3, slice)
        val expected = hex("0000000209f0" + "00000005060501ab80" + "0000000465888421" + "00000003419a02")
        converted(frame, expected, listOf(9, 6, 5, 1), strippedSps = 2, strippedPps = 2, strippedBytes = 20)
    }

    @Test
    fun `p3 a parameter set not in the configuration refuses the whole frame, wherever it is`() {
        outside(cat(sc4, hex("6742001eabce"), sc4, ppsA, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(0, 7))
        outside(cat(sc4, spsA, sc4, hex("68ce3c81"), sc4, idr), AvcPayloadRefusal.UnknownParameterSet(10, 8))
        outside(cat(sc4, spsA, sc4, ppsA, sc4, idr, sc4, hex("68ce3c81")), AvcPayloadRefusal.UnknownParameterSet(26, 8))
        // An SPS's bytes arriving as a PPS match nothing of that type.
        outside(cat(sc4, hex("68") + spsA.copyOfRange(1, spsA.size), sc4, idr), AvcPayloadRefusal.UnknownParameterSet(0, 8))
    }

    @Test
    fun `p3b identity is the exact length as well as every byte, for both types`() {
        // A known set plus a byte, a proper prefix of one, and one with its last byte changed.
        for (sps in listOf("6742001eabcdef", "6742001eab", "6742001eabce")) {
            outside(cat(sc4, hex(sps), sc4, ppsA, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(0, 7))
        }
        for (pps in listOf("68ce3c80ef", "68ce3c", "68ce3c81")) {
            outside(cat(sc4, spsA, sc4, hex(pps), sc4, idr), AvcPayloadRefusal.UnknownParameterSet(10, 8))
        }
    }

    @Test
    fun `p4 a parameter set after a three-byte prefix is malformed, identical or not`() {
        malformed(cat(sc4, aud, sc3, spsA, sc4, idr), AvcPayloadDefect.ThreeByteParameterSetStart(6))
        malformed(cat(sc4, idr, sc3, ppsA), AvcPayloadDefect.ThreeByteParameterSetStart(8))
    }

    @Test
    fun `p5 parameter-set headers and escapes are checked before identity`() {
        malformed(cat(sc4, hex("0742001eabcd"), sc4, idr), AvcPayloadDefect.ReferenceIndication(0, 7, 0))
        malformed(cat(sc4, spsA, sc4, hex("08ce3c80"), sc4, idr), AvcPayloadDefect.ReferenceIndication(10, 8, 0))
        malformed(cat(sc4, hex("e742001eabcd"), sc4, idr), AvcPayloadDefect.ForbiddenBitSet(0))
        malformed(cat(sc4, hex("67420000001ecd"), sc4, idr), AvcPayloadDefect.EmulationViolation(0))
    }

    @Test
    fun `p6 a frame with no coded slice is refused, never an empty or control-only frame`() {
        outside(cat(sc4, spsA, sc4, ppsA), AvcPayloadRefusal.NoVideoSlice)
        outside(cat(sc4, aud, sc4, sei), AvcPayloadRefusal.NoVideoSlice)
    }

    @Test
    fun `p7 the output size counts only retained NAL units`() {
        converted(cat(sc4, spsA, sc4, ppsA, sc4, idr), hex("0000000465888421"), listOf(5), AvcPayloadLimits(16, 8), 1, 1, 10)
        outside(cat(sc4, spsA, sc4, ppsA, sc4, idr), AvcPayloadRefusal.OutputTooLarge(8, 7), AvcPayloadLimits(16, 7))
    }

    @Test
    fun `p8 no caller or accessor alias can change a later comparison, for either type`() {
        val sps = spsA.copyOf()
        val pps = ppsA.copyOf()
        val owned = AvcParameterSetInventory.copyOf(listOf(sps), listOf(pps))
        sps[1] = 0x00
        pps[1] = 0x00
        val spsView = owned.sequenceParameterSet(0)
        val ppsView = owned.pictureParameterSet(0)
        spsView[2] = 0x7f
        ppsView[2] = 0x7f
        assertContentEquals(spsA, owned.sequenceParameterSet(0), "the inventory kept its own SPS")
        assertContentEquals(ppsA, owned.pictureParameterSet(0), "the inventory kept its own PPS")
        // The original bytes still match, with the exact output and one exact request.
        converted(cat(sc4, spsA, sc4, ppsA, sc4, idr), hex("0000000465888421"), listOf(5), strippedSps = 1, strippedPps = 1, strippedBytes = 10, config = owned)
        // Neither the caller's later edits nor an accessor's are in the inventory.
        outside(cat(sc4, sps, sc4, ppsA, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(0, 7), config = owned)
        outside(cat(sc4, spsA, sc4, pps, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(10, 8), config = owned)
        outside(cat(sc4, spsView, sc4, ppsA, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(0, 7), config = owned)
        outside(cat(sc4, spsA, sc4, ppsView, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(10, 8), config = owned)
    }

    @Test
    fun `p9 an allocation failure leaves as itself and a wrong-size allocation is a contract failure`() {
        val oom = OutOfMemoryError("output")
        val failing = Counting(failure = oom)
        val thrown = assertFailsWith<OutOfMemoryError> { AvcPayloads.convert(MediaBytes(cat(sc4, idr)), inventory, roomy, failing) }
        assertSame(oom, thrown, "the same object, not a verdict or a wrapper")
        assertEquals(listOf(8), failing.requests, "one request, of the exact size")
        for (delta in listOf(-1, 1)) {
            val wrong = Counting(sizeDelta = delta)
            assertFailsWith<IllegalStateException>("an allocation $delta bytes off") {
                AvcPayloads.convert(MediaBytes(cat(sc4, idr)), inventory, roomy, wrong)
            }
            assertEquals(listOf(8), wrong.requests, "one request, of the exact size")
        }
        // A refused frame never reaches the allocator, so a failing one is never called.
        val noSlice = Counting(failure = oom)
        val refused = AvcPayloads.convert(MediaBytes(cat(sc4, spsA, sc4, ppsA)), inventory, roomy, noSlice)
        assertEquals(AvcPayloadRefusal.NoVideoSlice, assertIs<AvcPayloadVerdict.OutsideSubset>(refused).refusal)
        assertEquals(emptyList(), noSlice.requests)
        // Nor does a frame whose unknown set comes after its slices.
        val late = Counting(failure = oom)
        val lateRefused = AvcPayloads.convert(MediaBytes(cat(sc4, spsA, sc4, ppsA, sc4, idr, sc4, spsB)), inventory, roomy, late)
        assertEquals(AvcPayloadRefusal.UnknownParameterSet(26, 7), assertIs<AvcPayloadVerdict.OutsideSubset>(lateRefused).refusal)
        assertEquals(emptyList(), late.requests)
    }

    @Test
    fun `p10 a match is any inventoried set of its type, not only the first`() {
        val two = AvcParameterSetInventory.copyOf(listOf(spsA, spsB), listOf(ppsA, ppsB))
        // Only the second of each: stripped, 6 + 4 NAL bytes.
        converted(cat(sc4, spsB, sc4, ppsB, sc4, idr), hex("0000000465888421"), listOf(5), strippedSps = 1, strippedPps = 1, strippedBytes = 10, config = two)
        // Both of each, in the other order, around a slice: 2 * (6 + 4) NAL bytes.
        converted(
            cat(sc4, aud, sc4, spsB, sc4, spsA, sc4, ppsB, sc4, ppsA, sc3, slice),
            hex("0000000209f0" + "00000003419a02"),
            listOf(9, 1),
            strippedSps = 2,
            strippedPps = 2,
            strippedBytes = 20,
            config = two,
        )
        // The single-set inventory does not know the second sets.
        outside(cat(sc4, spsB, sc4, ppsA, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(0, 7))
        outside(cat(sc4, spsA, sc4, ppsB, sc4, idr), AvcPayloadRefusal.UnknownParameterSet(10, 8))
    }
}
