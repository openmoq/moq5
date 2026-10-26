package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The interrupt-latch row of the bridge ABI, judged without naming the row in
 * code: by the authority header, the bound bytecode, and the shipped keep
 * rule. Adding a row changes the registered table, so the epoch moves with it.
 *
 * The C contract behind the row is `moq_endpoint_set_interrupted`: void and
 * any-thread. The row therefore LEASES like `wake` rather than acquiring, and
 * its only answer is MOQ_OK or a registry refusal, as an Int.
 */
internal class InterruptRowAuthorityTest {
    private companion object {
        const val NAME = "nativeEndpointSetInterrupted"
        const val DESCRIPTOR = "(JZ)I"
        const val EPOCH_WITH_ROW = 5

        /** The working directory of the unit tests is the module directory. */
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
    }

    @Test
    fun `the authority table carries the latch row with the leased descriptor`() {
        assertTrue(abiHeader.isFile, "setup: ${abiHeader.absolutePath}")
        val table = abiHeader.readText()
        assertTrue(
            Regex("""X\(\s*$NAME\s*,\s*"\Q$DESCRIPTOR\E"\s*\)""").containsMatchIn(table),
            "MOQ5_ABI_METHODS must carry X($NAME, \"$DESCRIPTOR\")",
        )
    }

    @Test
    fun `the bound bytecode carries the row as an instance public final native method`() {
        val m = NativeBridge::class.java.declaredMethods.filter { it.name == NAME }
        assertEquals(1, m.size, "exactly one $NAME on NativeBridge; found ${m.map { it.toGenericString() }}")
        val method = m.single()
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Boolean.TYPE), method.parameterTypes.toList())
        assertEquals(Integer.TYPE, method.returnType)
        val mods = method.modifiers
        assertTrue(Modifier.isPublic(mods) && Modifier.isFinal(mods) && Modifier.isNative(mods) && !Modifier.isStatic(mods),
            "the authority's chosen shape is public final native on the object instance")
    }

    @Test
    fun `the shipped keep rule carries the row so it survives R8`() {
        assertTrue(keepRules.isFile, "setup: ${keepRules.absolutePath}")
        assertTrue(
            keepRules.readLines().any { it.trim() == "public final native int $NAME(long, boolean);" },
            "consumer-rules.pro must keep $NAME by exact name and type",
        )
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val header = abiHeader.readText()
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE)
            .find(header)?.groupValues?.get(1)?.toInt()
        // The latch row moved the epoch to 5; later rows move it further, so
        // this pins a floor and the two sides' agreement, not the exact value.
        assertTrue(declared != null && declared >= EPOCH_WITH_ROW, "the authority's epoch is $declared, needs $EPOCH_WITH_ROW or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }
}
