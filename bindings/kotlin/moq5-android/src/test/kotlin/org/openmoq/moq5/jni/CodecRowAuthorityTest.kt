package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The two codec utility rows: in the authority with their descriptors, bound
 * as instance native methods, kept by the shipped rules, at ABI epoch 16 or
 * later on both sides.
 */
internal class CodecRowAuthorityTest {
    private companion object {
        const val EPOCH_WITH_ROWS = 16
        val ROWS =
            listOf(
                "nativeCodecInitDataBuild" to "(I[BI[[B)I",
                "nativeCodecStringFormat" to "(I[BZI[B[[B)I",
            )
        val KEEP =
            listOf(
                "public final native int nativeCodecInitDataBuild(int, byte[], int, byte[][]);",
                "public final native int nativeCodecStringFormat(int, byte[], boolean, int, byte[], byte[][]);",
            )
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
    }

    @Test
    fun `the authority table carries both codec rows with their descriptors, once each`() {
        val text = abiHeader.readText()
        for ((row, desc) in ROWS) {
            assertEquals(1, Regex("""X\(\s*$row\s*,\s*"\Q$desc\E"\s*\)""").findAll(text).count(), "MOQ5_ABI_METHODS must carry X($row, \"$desc\") exactly once")
        }
    }

    @Test
    fun `the bound bytecode carries both rows as instance public final native methods`() {
        for ((row, _) in ROWS) {
            val m = NativeBridge::class.java.declaredMethods.single { it.name == row }
            assertEquals(Integer.TYPE, m.returnType, row)
            assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers), row)
        }
        val build = NativeBridge::class.java.declaredMethods.single { it.name == "nativeCodecInitDataBuild" }
        assertEquals(listOf(Integer.TYPE, ByteArray::class.java, Integer.TYPE, Array<ByteArray>::class.java), build.parameterTypes.toList())
        val string = NativeBridge::class.java.declaredMethods.single { it.name == "nativeCodecStringFormat" }
        assertEquals(listOf(Integer.TYPE, ByteArray::class.java, java.lang.Boolean.TYPE, Integer.TYPE, ByteArray::class.java, Array<ByteArray>::class.java), string.parameterTypes.toList())
    }

    @Test
    fun `the shipped keep rules carry both rows`() {
        val lines = keepRules.readLines().map { it.trim() }
        for (k in KEEP) assertTrue(k in lines, k)
    }

    @Test
    fun `the ABI epoch advanced to 16 with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROWS, "the authority's epoch is $declared, needs $EPOCH_WITH_ROWS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }
}
