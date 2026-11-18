package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The receiver rows of the bridge ABI: attach under a root (an owned child
 * token published to `out[0]`) and close that child under its owner. Byte
 * identities cross as byte arrays. The table changed, so the epoch moved.
 */
internal class ReceiverRowAuthorityTest {
    private companion object {
        const val ATTACH = "nativeReceiverAttach"
        const val ATTACH_DESCRIPTOR = "(J[[BIIJIZ[BI[J)I"
        const val CLOSE = "nativeReceiverClose"
        const val CLOSE_DESCRIPTOR = "(JJ)I"
        const val EPOCH_WITH_ROWS = 9
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
    }

    @Test
    fun `the authority table carries both receiver rows with their descriptors`() {
        val table = abiHeader.readText()
        for ((name, desc) in listOf(ATTACH to ATTACH_DESCRIPTOR, CLOSE to CLOSE_DESCRIPTOR)) {
            assertTrue(Regex("""X\(\s*$name\s*,\s*"\Q$desc\E"\s*\)""").containsMatchIn(table), "MOQ5_ABI_METHODS must carry X($name, \"$desc\")")
        }
    }

    @Test
    fun `the bound bytecode carries both rows as instance public final native methods`() {
        val attach = NativeBridge::class.java.declaredMethods.single { it.name == ATTACH }
        assertEquals(
            listOf(java.lang.Long.TYPE, Array<ByteArray>::class.java, Integer.TYPE, Integer.TYPE, java.lang.Long.TYPE, Integer.TYPE, java.lang.Boolean.TYPE, ByteArray::class.java, Integer.TYPE, LongArray::class.java),
            attach.parameterTypes.toList(),
        )
        val close = NativeBridge::class.java.declaredMethods.single { it.name == CLOSE }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE), close.parameterTypes.toList())
        for (m in listOf(attach, close)) {
            assertEquals(Integer.TYPE, m.returnType)
            assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers))
        }
    }

    @Test
    fun `the shipped keep rules carry both rows so they survive R8`() {
        val lines = keepRules.readLines().map { it.trim() }
        assertTrue("public final native int $ATTACH(long, byte[][], int, int, long, int, boolean, byte[], int, long[]);" in lines)
        assertTrue("public final native int $CLOSE(long, long);" in lines)
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROWS, "the authority's epoch is $declared, needs $EPOCH_WITH_ROWS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }
}
