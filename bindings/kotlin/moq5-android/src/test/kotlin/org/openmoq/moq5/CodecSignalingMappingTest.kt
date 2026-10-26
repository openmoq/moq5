package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.CancellationException
import org.openmoq.moq5.jni.BridgeInit
import org.openmoq.moq5.jni.NativeBridge

/**
 * MAPPING tests: argument validation, the bridge decision gate and the
 * status/result mapping of [CodecSignalingCalls], over a SCRIPTED
 * [CodecNative] port. They prove nothing about the native codec utility or
 * the JNI rows; those are measured through the real host image elsewhere.
 */
class CodecSignalingMappingTest {

    private class Scripted(
        private val status: Int = MOQ_OK,
        private val result: ByteArray? = byteArrayOf(1, 2, 3),
        private val raise: Throwable? = null,
    ) : CodecNative {
        val calls = mutableListOf<List<Any?>>()
        var receivedSource: ByteArray? = null

        override fun initDataBuild(sourceFormat: Int, source: ByteArray, nalLengthSize: Int, out: Array<ByteArray?>): Int {
            calls.add(listOf("build", sourceFormat, source.toList(), nalLengthSize, out.size))
            receivedSource = source
            raise?.let { throw it }
            if (status == MOQ_OK) out[0] = result
            return status
        }

        override fun stringFormat(configFormat: Int, sampleEntry: ByteArray, hasOti: Boolean, oti: Int, decoderConfig: ByteArray, out: Array<ByteArray?>): Int {
            calls.add(listOf("string", configFormat, sampleEntry.toList(), hasOti, oti, decoderConfig.toList(), out.size))
            raise?.let { throw it }
            if (status == MOQ_OK) out[0] = result
            return status
        }
    }

    private var decisions = 0
    private val ready: () -> BridgeInit = { decisions++; BridgeInit.Ready(NativeBridge.ABI_EPOCH, NativeBridge.FEATURES_REQUIRED, 1) }

    private fun calls(port: CodecNative, decide: () -> BridgeInit = ready) = CodecSignalingCalls(decide, port)

    private val source = MediaBytes(byteArrayOf(0x12, 0x10))

    // ---- SampleEntry and native values ---------------------------------------------

    @Test
    fun m1_aSampleEntryIsExactlyFourPrintableAsciiCharactersWithExactEquality() {
        for (bad in listOf("avc", "avc1x", "", "av c", "avc\u0000", "avc\u007f", "avcé")) {
            assertFailsWith<IllegalArgumentException>("'$bad'") { SampleEntry(bad) }
        }
        assertEquals(SampleEntry("avc1"), SampleEntry("avc1"))
        assertEquals(SampleEntry("avc1").hashCode(), SampleEntry("avc1").hashCode())
        assertNotEquals(SampleEntry("avc1"), SampleEntry("Avc1"), "no case folding")
        assertNotEquals(SampleEntry("avc1"), SampleEntry("avc3"))
    }

    @Test
    fun m2_nativeValuesAreDeclaredNotOrdinals() {
        assertEquals(listOf(1, 2, 3), CodecConfigSource.entries.map { it.native })
        assertEquals(listOf(1, 2), CodecConfigFormat.entries.map { it.native })
    }

    // ---- representability before any native work -----------------------------------

    @Test
    fun m3_anUnrepresentableNalLengthSizeIsRefusedBeforeTheHandshakeOrAnyNativeCall() {
        for (n in listOf(257, 260, -255, -252, -1, 256)) {
            val port = Scripted()
            decisions = 0
            assertFailsWith<IllegalArgumentException>("nalLengthSize $n") { calls(port).buildInitData(CodecConfigSource.AvcAnnexB, source, n) }
            assertEquals(0, decisions, "no bridge decision for $n")
            assertEquals(emptyList(), port.calls, "no native call for $n")
        }
    }

    @Test
    fun m4_anUnrepresentableObjectTypeIndicationIsRefusedBeforeTheHandshakeOrAnyNativeCall() {
        for (oti in listOf(-1, 256, 320)) {
            val port = Scripted()
            decisions = 0
            assertFailsWith<IllegalArgumentException>("OTI $oti") { calls(port).formatCodecString(CodecConfigFormat.AacAudioSpecificConfig, SampleEntry("mp4a"), source, oti) }
            assertEquals(0, decisions)
            assertEquals(emptyList(), port.calls)
        }
    }

    @Test
    fun m5_representableValuesAreForwardedExactlyAndTheCValidatesTheirMeaning() {
        for (n in listOf(0, 1, 4, 5, 255)) {
            val port = Scripted(status = MOQ_ERR_INVAL)
            assertFailsWith<CodecSignalingException> { calls(port).buildInitData(CodecConfigSource.AvcAnnexB, source, n) }
            assertEquals(listOf(listOf<Any?>("build", 1, listOf<Byte>(0x12, 0x10), n, 1)), port.calls, "forwarded as given: $n")
        }
        val absent = Scripted(result = "mp4a.40.2".toByteArray(Charsets.US_ASCII))
        calls(absent).formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc3"), source, null)
        assertEquals(listOf(listOf<Any?>("string", 1, "avc3".toByteArray(Charsets.US_ASCII).toList(), false, 0, listOf<Byte>(0x12, 0x10), 1)), absent.calls, "an absent OTI crosses as false/0")
        val present = Scripted(result = "mp4a.40.2".toByteArray(Charsets.US_ASCII))
        calls(present).formatCodecString(CodecConfigFormat.AacAudioSpecificConfig, SampleEntry("mp4a"), source, 0x40)
        assertEquals(listOf(listOf<Any?>("string", 2, "mp4a".toByteArray(Charsets.US_ASCII).toList(), true, 0x40, listOf<Byte>(0x12, 0x10), 1)), present.calls)
    }

    // ---- the bridge decision -------------------------------------------------------

    @Test
    fun m6_anUnreadyBridgeIsThePublicInitializationFailureAndReachesNoNativeCall() {
        val failure = BridgeInit.LoadFailed("moq5_jni", null, "no such library")
        val port = Scripted()
        val thrown = assertFailsWith<NativeBridgeInitializationException> { calls(port) { failure }.buildInitData(CodecConfigSource.AvcRecord, source, 4) }
        assertEquals(NativeBridgeInitializationFailure.LoadFailed("moq5_jni", null, "no such library"), thrown.failure)
        assertEquals(emptyList(), port.calls)
        decisions = 0
        calls(Scripted()).buildInitData(CodecConfigSource.AvcRecord, source, 4)
        assertEquals(1, decisions, "one decision per call; the owner itself is what caches")
    }

    // ---- status and result mapping -------------------------------------------------

    @Test
    fun m7_eachCodecRefusalIsTheExceptionWithItsExactCode() {
        for (code in listOf(MOQ_ERR_INVAL, MOQ_ERR_PROTO, MOQ_ERR_UNSUPPORTED)) {
            val e = assertFailsWith<CodecSignalingException> { calls(Scripted(status = code)).buildInitData(CodecConfigSource.AvcAnnexB, source, 4) }
            assertEquals(MoqResultCode(code), e.code)
            val s = assertFailsWith<CodecSignalingException> { calls(Scripted(status = code)).formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), source, null) }
            assertEquals(MoqResultCode(code), s.code)
        }
    }

    @Test
    fun m8_allocationFailureContractFailureAndUnknownStatusesAreNeverCodecRefusals() {
        assertFailsWith<OutOfMemoryError> { calls(Scripted(status = MOQ_ERR_NOMEM)).buildInitData(CodecConfigSource.AvcAnnexB, source, 4) }
        assertFailsWith<OutOfMemoryError> { calls(Scripted(status = MOQ_ERR_NOMEM)).formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), source, null) }
        for (code in listOf(MOQ_ERR_INTERNAL, -5, -9, 7)) {
            val e = assertFailsWith<IllegalStateException>("status $code") { calls(Scripted(status = code)).buildInitData(CodecConfigSource.AvcAnnexB, source, 4) }
            assertEquals<Class<*>>(IllegalStateException::class.java, e.javaClass, "a contract failure, not a refusal: status $code")
        }
    }

    @Test
    fun m9_okWithoutAUsableResultIsAContractFailure() {
        assertFailsWith<IllegalStateException> { calls(Scripted(result = null)).buildInitData(CodecConfigSource.AvcRecord, source, 4) }
        assertFailsWith<IllegalStateException> { calls(Scripted(result = ByteArray(0))).buildInitData(CodecConfigSource.AvcRecord, source, 4) }
        assertFailsWith<IllegalStateException> { calls(Scripted(result = null)).formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), source, null) }
        for (bad in listOf(byteArrayOf(0x61, 0x80.toByte()), byteArrayOf(0x61, 0x20), byteArrayOf(0x61, 0x7f), byteArrayOf(0x61, 0x00))) {
            val e = assertFailsWith<IllegalStateException>("bytes ${bad.toList()}") { calls(Scripted(result = bad)).formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), source, null) }
            assertTrue(e.message!!.contains("non-ASCII"), e.message)
        }
    }

    @Test
    fun m10_aSuccessfulResultIsExactAndIndependentOfTheNativeArray() {
        val native = byteArrayOf(9, 8, 7)
        val built = calls(Scripted(result = native)).buildInitData(CodecConfigSource.AvcRecord, source, 4)
        assertContentEquals(byteArrayOf(9, 8, 7), built.bytes())
        native[0] = 0
        assertContentEquals(byteArrayOf(9, 8, 7), built.bytes(), "the result does not alias the native array")
        assertEquals("avc1.42001e", calls(Scripted(result = "avc1.42001e".toByteArray(Charsets.US_ASCII))).formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), source, null))
    }

    @Test
    fun m11_theCallersInputIsNeverTheArrayThePortReceives() {
        val port = Scripted()
        val input = MediaBytes(byteArrayOf(1, 2, 3, 4))
        calls(port).buildInitData(CodecConfigSource.AvcRecord, input, 4)
        port.receivedSource!![0] = 99
        assertContentEquals(byteArrayOf(1, 2, 3, 4), input.bytes(), "the port's array is a copy")
    }

    @Test
    fun m12_aThrowableFromThePortLeavesAsItself() {
        for (t in listOf<Throwable>(OutOfMemoryError("declared"), IllegalStateException("declared"), CancellationException("declared"), AssertionError("declared"))) {
            val thrown = runCatching { calls(Scripted(raise = t)).buildInitData(CodecConfigSource.AvcRecord, source, 4) }.exceptionOrNull()
            assertSame(t, thrown, "identity for ${t::class.simpleName}")
            val s = runCatching { calls(Scripted(raise = t)).formatCodecString(CodecConfigFormat.AvcRecord, SampleEntry("avc1"), source, null) }.exceptionOrNull()
            assertSame(t, s)
        }
    }

    @Test
    fun m13_theRefusalExceptionIsNotAnAllocationOrStateFailure() {
        val e = assertFailsWith<CodecSignalingException> { calls(Scripted(status = MOQ_ERR_PROTO)).buildInitData(CodecConfigSource.AvcRecord, source, 4) }
        assertEquals<Class<*>?>(IllegalArgumentException::class.java, e.javaClass.superclass, "a refusal is an argument failure, never a state or allocation failure")
    }

    private companion object {
        const val MOQ_OK = 0
        const val MOQ_ERR_NOMEM = -1
        const val MOQ_ERR_INVAL = -2
        const val MOQ_ERR_PROTO = -3
        const val MOQ_ERR_UNSUPPORTED = -14
        const val MOQ_ERR_INTERNAL = -99
    }
}
