package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The receiver STATS row: one `moq_media_receiver_get_stats` read on the
 * receiver child, every field published exactly once into a fixed scalar
 * block in the struct's order.
 */
internal class ReceiverStatsRowAuthorityTest {
    private companion object {
        const val ROW = "nativeReceiverStats"
        const val DESCRIPTOR = "(JJ[J)I"
        const val EPOCH_WITH_ROW = 14
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
        val preflight = File("../tools/check-jni-endpoint-preflight.sh")
    }

    @Test
    fun `the authority table carries the stats row with its descriptor`() {
        assertTrue(Regex("""X\(\s*$ROW\s*,\s*"\Q$DESCRIPTOR\E"\s*\)""").containsMatchIn(abiHeader.readText()), "MOQ5_ABI_METHODS must carry X($ROW, \"$DESCRIPTOR\")")
    }

    @Test
    fun `the bound bytecode carries the row as an instance public final native method`() {
        val m = NativeBridge::class.java.declaredMethods.single { it.name == ROW }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, LongArray::class.java), m.parameterTypes.toList())
        assertEquals(Integer.TYPE, m.returnType)
        assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers))
    }

    @Test
    fun `the shipped keep rules carry the row`() {
        assertTrue("public final native int $ROW(long, long, long[]);" in keepRules.readLines().map { it.trim() })
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROW, "the authority's epoch is $declared, needs $EPOCH_WITH_ROW or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the preflight names the stats substrate`() {
        assertTrue(Regex("""RX_SET='[^']*\bmoq_media_receiver_get_stats\b[^']*'""").containsMatchIn(preflight.readText()))
    }
}
