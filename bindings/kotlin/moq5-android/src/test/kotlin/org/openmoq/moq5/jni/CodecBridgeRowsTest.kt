package org.openmoq.moq5.jni

import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.CodecSignaling
import org.openmoq.moq5.CodecSignalingException
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.SampleEntry

/**
 * The two codec rows through the REAL host product image: the public object
 * (and the process-wide owner) for the codec contract, the registered rows
 * directly for the bridge's own argument, holder, exception and allocation
 * discipline. The recorder logs every call the rows make into the utility --
 * every field and the exact input and entry bytes -- and forwards it to the
 * real one; synthetic results, JNI faults and allocation faults are armed
 * only where a row says so. Expected bytes and strings are the reviewed native
 * vectors ([CodecVectors]). Each parameterized row runs every subcase and
 * prints which executed.
 */
internal class CodecBridgeRowsTest {
    @BeforeTest
    fun load() {
        assertIs<BridgeInit.Ready>(NativeBridgeOwner.initialize(), "setup: the host product image must load")
        TrustRouteProbe.nativeCodecReset()
    }

    @AfterTest
    fun noInstrumentFault() {
        assertEquals(null, TrustRouteProbe.nativeFault(), "the recorder latched an instrumentation fault")
    }

    private val ascii = Charsets.US_ASCII
    private val avc1 = "avc1".toByteArray(ascii)
    private val avc3 = "avc3".toByteArray(ascii)
    private val mp4a = "mp4a".toByteArray(ascii)

    private fun refused(kind: Long, format: Long, lengthSize: Long, rc: Int, input: ByteArray, entry: ByteArray = ByteArray(0), hasOti: Long = 0, oti: Long = 0) =
        ExpectedCall(CodecCall(kind, format, lengthSize, hasOti, oti, 0, rc.toLong(), 0, input.size.toLong(), entry.size.toLong(), 0), input, entry)

    // ---- the codec contract through the public object ----------------------------

    @Test
    fun c1_anAnnexBSourceBuildsTheReviewedRecordWithOneQueryAndOneBuild() {
        assertContentEquals(CodecVectors.AVCC, CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(CodecVectors.AVC_ANNEXB)).bytes())
        expectCodecCalls(queryAndBuild(1, 1, 4, 21, CodecVectors.AVC_ANNEXB), "annex-B build")
    }

    @Test
    fun c2_theAvcCodecStringKeepsTheExactSampleEntry() {
        assertEquals("avc1.42001e", CodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), MediaBytes(CodecVectors.AVCC)))
        assertEquals("avc3.42001e", CodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc3"), MediaBytes(CodecVectors.AVCC)))
        expectCodecCalls(queryAndBuild(2, 1, -1, 11, CodecVectors.AVCC, avc1) + queryAndBuild(2, 1, -1, 11, CodecVectors.AVCC, avc3), "avc strings")
    }

    @Test
    fun c3_anAudioSpecificConfigIsCopiedAndTwoDifferentConfigsShareOneString() {
        assertContentEquals(CodecVectors.ASC_LC, CodecSignaling.buildInitData(CodecConfigSource.AacAudioSpecificConfig, MediaBytes(CodecVectors.ASC_LC)).bytes())
        assertEquals("mp4a.40.2", CodecSignaling.formatCodecString(CodecConfigFormat.AacAudioSpecificConfig, SampleEntry("mp4a"), MediaBytes(CodecVectors.ASC_LC), 0x40))
        assertEquals("mp4a.40.2", CodecSignaling.formatCodecString(CodecConfigFormat.AacAudioSpecificConfig, SampleEntry("mp4a"), MediaBytes(CodecVectors.ASC_LC_960), 0x40))
        expectCodecCalls(
            queryAndBuild(1, 3, 4, 2, CodecVectors.ASC_LC) +
                queryAndBuild(2, 2, -1, 9, CodecVectors.ASC_LC, mp4a, hasOti = 1, oti = 0x40) +
                queryAndBuild(2, 2, -1, 9, CodecVectors.ASC_LC_960, mp4a, hasOti = 1, oti = 0x40),
            "the six AAC calls, with DIFFERENT inputs behind the same string",
        )
    }

    @Test
    fun c4_aMissingParameterSetIsProtoFromTheSizeQueryAlone() {
        val e = assertFailsWith<CodecSignalingException> { CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(CodecVectors.AVC_SPS_ONLY)) }
        assertEquals(MoqResultCode(CodecVectors.MOQ_ERR_PROTO), e.code)
        expectCodecCalls(listOf(refused(1, 1, 4, CodecVectors.MOQ_ERR_PROTO, CodecVectors.AVC_SPS_ONLY)), "one query, refused; no build")
    }

    @Test
    fun c5_tooManyParameterSetsIsUnsupportedFromTheRealUtility() {
        val e = assertFailsWith<CodecSignalingException> { CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(CodecVectors.AVC_SEVENTEEN_SPS)) }
        assertEquals(MoqResultCode(CodecVectors.MOQ_ERR_UNSUPPORTED), e.code)
        expectCodecCalls(listOf(refused(1, 1, 4, CodecVectors.MOQ_ERR_UNSUPPORTED, CodecVectors.AVC_SEVENTEEN_SPS)), "the UTILITY refused it, once")
    }

    @Test
    fun c6_aSampleEntryThatDoesNotMatchItsFormatIsInvalFromTheUtility() {
        val e = assertFailsWith<CodecSignalingException> { CodecSignaling.formatCodecString(CodecConfigFormat.AacAudioSpecificConfig, SampleEntry("avc1"), MediaBytes(CodecVectors.ASC_LC), 0x40) }
        assertEquals(MoqResultCode(CodecVectors.MOQ_ERR_INVAL), e.code)
        expectCodecCalls(listOf(refused(2, 2, -1, CodecVectors.MOQ_ERR_INVAL, CodecVectors.ASC_LC, avc1, hasOti = 1, oti = 0x40)), "one query, refused")
    }

    @Test
    fun c7_theNalLengthSizeReachesTheUtilityWhichDecidesItsMeaning() {
        val cases = mutableListOf<Pair<String, () -> Unit>>()
        for (n in listOf(0, 5)) {
            cases += "nalLengthSize $n refused by the utility" to {
                TrustRouteProbe.nativeCodecReset()
                val e = assertFailsWith<CodecSignalingException> { CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(CodecVectors.AVC_ANNEXB), n) }
                assertEquals(MoqResultCode(CodecVectors.MOQ_ERR_INVAL), e.code)
                expectCodecCalls(listOf(refused(1, 1, n.toLong(), CodecVectors.MOQ_ERR_INVAL, CodecVectors.AVC_ANNEXB)), "nal $n")
            }
        }
        for ((n, marker) in listOf(1 to 0xFC, 2 to 0xFD)) {
            cases += "nalLengthSize $n built" to {
                TrustRouteProbe.nativeCodecReset()
                val expected = CodecVectors.AVCC.copyOf().also { it[4] = marker.toByte() }
                assertContentEquals(expected, CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(CodecVectors.AVC_ANNEXB), n).bytes())
                expectCodecCalls(queryAndBuild(1, 1, n.toLong(), 21, CodecVectors.AVC_ANNEXB), "nal $n")
            }
        }
        subcases("c7", cases)
    }

    /** The RECORD source (value 2): validated and copied, with an in-range length size ignored. Not the formatter. */
    @Test
    fun c13_anAvcRecordSourceIsCopiedAndItsInRangeLengthSizeIsIgnored() {
        subcases(
            "c13",
            listOf(4, 0, 7, 255).map { n ->
                "record source, nalLengthSize $n" to {
                    TrustRouteProbe.nativeCodecReset()
                    assertContentEquals(CodecVectors.AVCC, CodecSignaling.buildInitData(CodecConfigSource.AvcRecord, MediaBytes(CodecVectors.AVCC), n).bytes())
                    expectCodecCalls(queryAndBuild(1, 2, n.toLong(), 21, CodecVectors.AVCC), "record source $n")
                }
            },
        )
    }

    // ---- the rows themselves, over BOTH entry points --------------------------------

    /** One JNI entry point, as a row sees it: its valid call, its success, its declared calls and allocations. */
    private class Row(
        val name: String,
        val inputs: Int,
        val call: (Array<ByteArray?>?) -> Int,
        val publicCall: () -> Unit,
        val success: ByteArray,
        val calls: List<ExpectedCall>,
        /** The declared native input allocations, in order, then the output allocation. */
        val inputAllocations: List<Long>,
        val outputAllocation: Long,
    ) {
        val allocations: List<Long> get() = inputAllocations + outputAllocation
    }

    private val buildRow =
        Row(
            "build", 1,
            { h -> NativeBridge.nativeCodecInitDataBuild(1, CodecVectors.AVC_ANNEXB, 4, h) },
            { CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(CodecVectors.AVC_ANNEXB)) },
            CodecVectors.AVCC,
            queryAndBuild(1, 1, 4, 21, CodecVectors.AVC_ANNEXB),
            listOf(18L), 21L,
        )
    private val stringRow =
        Row(
            "string", 2,
            { h -> NativeBridge.nativeCodecStringFormat(1, avc1, false, 0, CodecVectors.AVCC, h) },
            { CodecSignaling.formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), MediaBytes(CodecVectors.AVCC)) },
            "avc1.42001e".toByteArray(ascii),
            queryAndBuild(2, 1, -1, 11, CodecVectors.AVCC, avc1),
            listOf(4L, 21L), 11L,
        )
    private val rows = listOf(buildRow, stringRow)

    @Test
    fun c8_theRowsRefuseUnrepresentableOrUnknownArgumentsWithoutCallingTheUtility() {
        val sentinel = byteArrayOf(0x55)
        fun holder() = arrayOf<ByteArray?>(sentinel)
        val src = CodecVectors.AVC_ANNEXB
        val cases = mutableListOf<Pair<String, () -> Pair<Int, Array<ByteArray?>?>>>()
        for (n in listOf(257, 260, -255, -252, -1, 256)) cases += "build nalLengthSize $n" to { val h = holder(); NativeBridge.nativeCodecInitDataBuild(1, src, n, h) to h }
        for (f in listOf(0, 4, 9, -1)) cases += "build sourceFormat $f" to { val h = holder(); NativeBridge.nativeCodecInitDataBuild(f, src, 4, h) to h }
        for (f in listOf(0, 3, 5)) cases += "string configFormat $f" to { val h = holder(); NativeBridge.nativeCodecStringFormat(f, avc1, false, 0, CodecVectors.AVCC, h) to h }
        for (o in listOf(-1, 256, 320)) cases += "string oti $o" to { val h = holder(); NativeBridge.nativeCodecStringFormat(2, mp4a, true, o, CodecVectors.ASC_LC, h) to h }
        cases += "string absent oti with a value" to { val h = holder(); NativeBridge.nativeCodecStringFormat(1, avc1, false, 5, CodecVectors.AVCC, h) to h }
        cases += "build null source" to { val h = holder(); NativeBridge.nativeCodecInitDataBuild(1, null, 4, h) to h }
        cases += "string null entry" to { val h = holder(); NativeBridge.nativeCodecStringFormat(1, null, false, 0, CodecVectors.AVCC, h) to h }
        cases += "string null decoder config" to { val h = holder(); NativeBridge.nativeCodecStringFormat(1, avc1, false, 0, null, h) to h }
        cases += "build null holder" to { NativeBridge.nativeCodecInitDataBuild(1, src, 4, null) to null }
        cases += "build empty holder" to { NativeBridge.nativeCodecInitDataBuild(1, src, 4, arrayOfNulls(0)) to null }
        cases += "string null holder" to { NativeBridge.nativeCodecStringFormat(1, avc1, false, 0, CodecVectors.AVCC, null) to null }
        cases += "string empty holder" to { NativeBridge.nativeCodecStringFormat(1, avc1, false, 0, CodecVectors.AVCC, arrayOfNulls(0)) to null }
        subcases(
            "c8",
            cases.map { (name, run) ->
                name to {
                    TrustRouteProbe.nativeCodecReset()
                    val (rc, h) = run()
                    assertEquals(CodecVectors.MOQ_ERR_INVAL, rc, name)
                    assertEquals(emptyList(), CodecCall.all(), "$name: the utility was not called")
                    if (h != null) assertSame(sentinel, h[0], "$name: the holder is untouched")
                }
            },
        )
    }

    @Test
    fun c9_aUtilityRefusalLeavesTheHolderUntouchedAndRetiresEveryInput() {
        val sentinel = byteArrayOf(0x55)
        fun refusal(name: String, expectedRc: Int, calls: List<ExpectedCall>, allocations: List<Long>, call: (Array<ByteArray?>) -> Int) =
            name to {
                TrustRouteProbe.nativeCodecReset()
                val h = arrayOf<ByteArray?>(sentinel)
                val o = observed(allocFailAt = -1) { call(h) }
                expectAllocations(allocations.map { it to ALLOC_RETIRED }, o.alloc, "$name: every input retired, no output allocated")
                assertEquals(expectedRc, o.get(), name)
                assertSame(sentinel, h[0], "$name: the holder is untouched")
                expectCodecCalls(calls, name)
            }
        subcases(
            "c9",
            listOf(
                refusal("build refusal", CodecVectors.MOQ_ERR_PROTO, listOf(refused(1, 1, 4, CodecVectors.MOQ_ERR_PROTO, CodecVectors.AVC_SPS_ONLY)), listOf(10L)) {
                    NativeBridge.nativeCodecInitDataBuild(1, CodecVectors.AVC_SPS_ONLY, 4, it)
                },
                // An empty input still crosses as a NON-NULL native pointer: a one-byte owned allocation.
                refusal("build empty input", CodecVectors.MOQ_ERR_PROTO, listOf(refused(1, 1, 4, CodecVectors.MOQ_ERR_PROTO, ByteArray(0))), listOf(1L)) {
                    NativeBridge.nativeCodecInitDataBuild(1, ByteArray(0), 4, it)
                },
                refusal("string refusal", CodecVectors.MOQ_ERR_INVAL, listOf(refused(2, 2, -1, CodecVectors.MOQ_ERR_INVAL, CodecVectors.ASC_LC, avc1, hasOti = 1, oti = 0x40)), listOf(4L, 2L)) {
                    NativeBridge.nativeCodecStringFormat(2, avc1, true, 0x40, CodecVectors.ASC_LC, it)
                },
                refusal("string empty decoder config", CodecVectors.MOQ_ERR_PROTO, listOf(refused(2, 1, -1, CodecVectors.MOQ_ERR_PROTO, ByteArray(0), avc1)), listOf(4L, 1L)) {
                    NativeBridge.nativeCodecStringFormat(1, avc1, false, 0, ByteArray(0), it)
                },
            ),
        )
    }

    @Test
    fun c10_everyInconsistentUtilityResultIsAContractFailureOnBothRows() {
        // (call index, synthetic rc, synthetic out_len, calls made); ROW_LEN is
        // the row's real output length, so only the status is inconsistent.
        val synthetic =
            listOf(
                listOf<Any>(0, CodecVectors.MOQ_OK, 0L, 1, "the size query answered OK"),
                listOf<Any>(0, CodecVectors.MOQ_OK, ROW_LEN, 1, "the size query answered OK with the real size"),
                listOf<Any>(0, CodecVectors.MOQ_ERR_BUFFER, 0L, 1, "a zero required size"),
                listOf<Any>(0, CodecVectors.MOQ_ERR_BUFFER, 0x8000_0000L, 1, "a size no Java array can hold"),
                listOf<Any>(0, CodecVectors.MOQ_ERR_INVAL, 5L, 1, "a refusal that reports a length"),
                listOf<Any>(0, -5, 0L, 1, "a status this operation does not define"),
                listOf<Any>(1, CodecVectors.MOQ_ERR_PROTO, 0L, 2, "the build disagreeing with its query"),
                listOf<Any>(1, CodecVectors.MOQ_OK, 1L, 2, "a result of the wrong length"),
                listOf<Any>(1, CodecVectors.MOQ_ERR_BUFFER, ROW_LEN, 2, "the build answered BUFFER for its exact size"),
            )
        val cases = mutableListOf<Pair<String, () -> Unit>>()
        for (row in rows) for (s in synthetic) {
            val (at, rc, declaredLen, calls, what) = s
            val len = if (declaredLen == ROW_LEN) row.outputAllocation else declaredLen as Long
            cases += "${row.name}: $what" to {
                TrustRouteProbe.nativeCodecReset()
                TrustRouteProbe.nativeCodecInject(at as Int, rc as Int, len)
                val sentinel = byteArrayOf(0x55)
                val h = arrayOf<ByteArray?>(sentinel)
                val o = observed(allocFailAt = -1) { row.call(h) }
                // A query-phase failure allocates the inputs only; a build-phase one also the real-sized output.
                val declared = if (at == 0) row.inputAllocations else row.allocations
                expectAllocations(declared.map { it to ALLOC_RETIRED }, o.alloc, "every native allocation retired")
                assertEquals(CodecVectors.MOQ_ERR_INTERNAL, o.get(), "direct")
                assertSame(sentinel, h[0], "nothing published")
                assertEquals(calls as Int, CodecCall.all().size, "calls made")
                TrustRouteProbe.nativeCodecReset()
                TrustRouteProbe.nativeCodecInject(at, rc, len)
                assertFailsWith<IllegalStateException>("public") { row.publicCall() }
            }
        }
        subcases("c10", cases)
    }

    private class Declared(message: String) : RuntimeException(message)

    private companion object {
        /** In a synthetic c10 case: the row's own real output length. */
        const val ROW_LEN = -1L
    }

    /**
     * Arms [site] after [skip] occurrences and the allocation ledger, runs
     * [call] through the scoped observer, and returns (thrown, returned
     * status, JNI log, allocation ledger), all settled.
     */
    private fun faulted(site: Int, skip: Int = 0, call: () -> Int): Pair<Triple<Throwable?, Int?, IntArray>, AllocLedger?> {
        val declared = Declared("the declared fault at $site")
        val o = observed(allocFailAt = -1, envSite = site, envSkip = skip, envThrowable = declared) { call() }
        val thrown = o.result.exceptionOrNull()
        if (thrown != null) assertSame(declared, thrown, "the declared Throwable leaves by identity at site $site")
        return Triple(thrown, o.result.getOrNull(), o.env!!) to o.alloc
    }

    @Test
    fun c11_aPendingExceptionAtEachSiteStopsEitherRowThereAndLeavesAsItself() {
        val gr = TrustRouteProbe.OP_GET_REGION
        val na = TrustRouteProbe.OP_NEW_ARRAY
        val sr = TrustRouteProbe.OP_SET_REGION
        val se = TrustRouteProbe.OP_SET_ELEMENT
        val ec = TrustRouteProbe.OP_EXCEPTION_CHECK
        val dl = TrustRouteProbe.OP_DELETE_LOCAL
        val cases = mutableListOf<Pair<String, () -> Unit>>()
        for (row in rows) {
            val copies = (0 until row.inputs).flatMap { listOf(gr, ec) }
            val tail = listOf(na, sr, ec, se, ec, dl)
            cases += "${row.name}: no fault" to {
                TrustRouteProbe.nativeCodecReset()
                val h = arrayOfNulls<ByteArray>(1)
                val (obs, alloc) = faulted(0) { row.call(h) }
                val (t, r, log) = obs
                expectAllocations(row.allocations.map { it to ALLOC_RETIRED }, alloc, "${row.name} success")
                assertNull(t)
                assertEquals(CodecVectors.MOQ_OK, r)
                assertContentEquals((listOf(0) + copies + tail).toIntArray(), log, "the whole sequence, in order")
                assertContentEquals(row.success, h[0])
                expectCodecCalls(row.calls, "${row.name} success")
            }
            for (k in 0 until row.inputs) {
                cases += "${row.name}: input copy ${k + 1} faulted" to {
                    TrustRouteProbe.nativeCodecReset()
                    val h = arrayOfNulls<ByteArray>(1)
                    val (obs, alloc) = faulted(gr, skip = k) { row.call(h) }
                    val (t, _, log) = obs
                    expectAllocations(row.inputAllocations.take(k + 1).map { it to ALLOC_RETIRED }, alloc, "inputs allocated up to the faulted copy, all retired")
                    assertTrue(t != null, "the declared Throwable left")
                    assertContentEquals((listOf(1) + copies.take(2 * (k + 1))).toIntArray(), log, "copies up to the faulted one, nothing after")
                    assertEquals(emptyList(), CodecCall.all(), "the utility was not called")
                    assertNull(h[0])
                }
            }
            for ((site, upto) in listOf(na to listOf(na, ec), sr to listOf(na, sr, ec, dl), se to listOf(na, sr, ec, se, ec, dl))) {
                cases += "${row.name}: output site $site faulted" to {
                    TrustRouteProbe.nativeCodecReset()
                    val h = arrayOfNulls<ByteArray>(1)
                    val (obs, alloc) = faulted(site) { row.call(h) }
                    val (t, _, log) = obs
                    expectAllocations(row.allocations.map { it to ALLOC_RETIRED }, alloc, "inputs and output retired on the output site's failure")
                    assertTrue(t != null, "the declared Throwable left")
                    assertContentEquals((listOf(1) + copies + upto).toIntArray(), log, "reached, fired, and nothing after it")
                    expectCodecCalls(row.calls, "${row.name} site $site: the utility ran exactly its query and build")
                    assertNull(h[0], "nothing published")
                }
            }
            cases += "${row.name}: NewByteArray NULL with nothing pending" to {
                TrustRouteProbe.nativeCodecReset()
                val sentinel = byteArrayOf(0x55)
                val h = arrayOf<ByteArray?>(sentinel)
                val (obs, alloc) = faulted(TrustRouteProbe.SITE_NEW_ARRAY_NULL) { row.call(h) }
                val (t, r, log) = obs
                expectAllocations(row.allocations.map { it to ALLOC_RETIRED }, alloc, "inputs and output retired on a JVM allocation failure")
                assertNull(t, "nothing pending")
                assertEquals(CodecVectors.MOQ_ERR_NOMEM, r, "a JVM allocation failure without an exception")
                assertContentEquals((listOf(1) + copies + listOf(na, ec)).toIntArray(), log)
                assertSame(sentinel, h[0])
            }
        }
        subcases("c11", cases)
    }

    /** Each native allocation a row makes, forced to fail in turn: NOMEM, the holder untouched, earlier blocks retired, no later attempt. */
    @Test
    fun c12_eachForcedAllocationFailureRetiresWhatCameBeforeOnBothRows() {
        val cases = mutableListOf<Pair<String, () -> Unit>>()
        for (row in rows) {
            for (at in row.allocations.indices) {
                cases += "${row.name}: allocation ${at + 1} fails" to {
                    TrustRouteProbe.nativeCodecReset()
                    val sentinel = byteArrayOf(0x55)
                    val h = arrayOf<ByteArray?>(sentinel)
                    val o = observed(allocFailAt = at) { row.call(h) }
                    val expected = row.allocations.take(at).map { it to ALLOC_RETIRED } + (row.allocations[at] to ALLOC_FAILED)
                    expectAllocations(expected, o.alloc, "${row.name} failure at ${at + 1}: earlier blocks retired, no later attempt")
                    assertEquals(CodecVectors.MOQ_ERR_NOMEM, o.get())
                    assertSame(sentinel, h[0], "the holder is untouched")
                    assertEquals(if (at < row.inputs) 0 else 1, CodecCall.all().size, "utility calls before the failure")
                }
            }
        }
        subcases("c12", cases)
    }
}
