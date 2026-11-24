package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The receiver EVENT rows: drain one track event from a receiver child (a
 * scalar block; a track handle crosses as a BORROWED child token minted on
 * first sight) and describe a track (the scalar block plus the byte fields
 * as byte arrays, returned; null with the code in the scalars on refusal).
 */
internal class ReceiverEventRowAuthorityTest {
    private companion object {
        const val POLL = "nativeReceiverPollTrack"
        const val POLL_DESCRIPTOR = "(JJ[J)I"
        const val DESCRIBE = "nativeTrackDescribe"
        const val DESCRIBE_DESCRIPTOR = "(JJJ[J)[[B"
        const val EPOCH_WITH_ROWS = 10
        const val EPOCH_WITH_APPENDED_FIELDS = 18
        const val EPOCH_WITH_REPLACEMENT = 19
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
    }

    @Test
    fun `the authority table carries both event rows with their descriptors`() {
        val table = abiHeader.readText()
        for ((name, desc) in listOf(POLL to POLL_DESCRIPTOR, DESCRIBE to DESCRIBE_DESCRIPTOR)) {
            assertTrue(Regex("""X\(\s*$name\s*,\s*"\Q$desc\E"\s*\)""").containsMatchIn(table), "MOQ5_ABI_METHODS must carry X($name, \"$desc\")")
        }
    }

    @Test
    fun `the bound bytecode carries both rows as instance public final native methods`() {
        val poll = NativeBridge::class.java.declaredMethods.single { it.name == POLL }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, LongArray::class.java), poll.parameterTypes.toList())
        assertEquals(Integer.TYPE, poll.returnType)
        val describe = NativeBridge::class.java.declaredMethods.single { it.name == DESCRIBE }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, java.lang.Long.TYPE, LongArray::class.java), describe.parameterTypes.toList())
        assertEquals(Array<ByteArray>::class.java, describe.returnType)
        for (m in listOf(poll, describe)) {
            assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers))
        }
    }

    @Test
    fun `the shipped keep rules carry both rows so they survive R8`() {
        val lines = keepRules.readLines().map { it.trim() }
        assertTrue("public final native int $POLL(long, long, long[]);" in lines)
        assertTrue("public final native byte[][] $DESCRIBE(long, long, long, long[]);" in lines)
    }

    @Test
    fun `the track-event carrier's appended-field scalars moved the epoch past the last 12-scalar epoch`() {
        assertTrue(ReceiverEventScalars.COUNT >= 23, "the appended-field slots are still carried")
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_APPENDED_FIELDS, "the authority's epoch is $declared, needs $EPOCH_WITH_APPENDED_FIELDS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the replacement and generation slots moved the epoch past the 23-scalar track event and the 35-scalar describe`() {
        assertEquals(26, ReceiverEventScalars.COUNT)
        assertEquals(39, TrackDescribeScalars.COUNT)
        assertEquals(17, TrackDescribeBytes.FIXED)
        assertEquals(27, ReceiverStatsScalars.COUNT)
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_REPLACEMENT, "the authority's epoch is $declared, needs $EPOCH_WITH_REPLACEMENT or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the native carriers declare the counts the Kotlin carriers name`() {
        val jni = File("../jni/moq5_jni.c").readText()
        fun define(name: String): Int? = Regex("""^#define $name (\d+)$""", RegexOption.MULTILINE).find(jni)?.groupValues?.get(1)?.toInt()
        assertEquals(ReceiverEventScalars.COUNT, define("MOQ5_RX_EV_SCALARS"))
        assertEquals(TrackDescribeScalars.COUNT, define("MOQ5_TD_SCALARS"))
        assertEquals(TrackDescribeBytes.FIXED, define("MOQ5_TD_FIXED_BYTES"))
        assertEquals(ReceiverStatsScalars.COUNT, define("MOQ5_RX_STATS_SCALARS"))
        assertEquals(ReceiverEventScalars.HAS_REPLACEMENT, Regex("""^#define MOQ5_EV_HAS_REPLACEMENT +0x([0-9a-fA-F]+)$""", RegexOption.MULTILINE).find(jni)?.groupValues?.get(1)?.toLong(16))
    }

    @Test
    fun `the preflight names the extension lookup the describe row reads the encryption fields through`() {
        val text = File("../tools/check-jni-endpoint-preflight.sh").readText()
        assertTrue(Regex("""RX_SET='[^']*\bmoq_msf_ext_find\b[^']*'""").containsMatchIn(text), "RX_SET must name moq_msf_ext_find so both Android images prove it by identity")
        assertTrue(Regex("""(?m)^RX_N=19$""").containsMatchIn(text), "and count it")
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROWS, "the authority's epoch is $declared, needs $EPOCH_WITH_ROWS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }
}
