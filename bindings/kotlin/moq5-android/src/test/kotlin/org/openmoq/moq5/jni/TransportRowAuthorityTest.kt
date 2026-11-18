package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The trusted create row carrying transport selection too: the protocol
 * ordinal and the WebTransport path (null = no override) follow the offer
 * and the handshake bound. One create row; the descriptor changes, so the
 * epoch moves.
 */
internal class TransportRowAuthorityTest {
    private companion object {
        const val NAME = "nativeEndpointCreateTrusted"
        const val DESCRIPTOR = "(Ljava/lang/String;II[IJILjava/lang/String;[J)I"
        const val EPOCH_WITH_ROW = 8
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
    }

    @Test
    fun `the authority table carries the trusted create row with the transport descriptor`() {
        assertTrue(abiHeader.isFile, "setup: ${abiHeader.absolutePath}")
        assertTrue(
            Regex("""X\(\s*$NAME\s*,\s*"\Q$DESCRIPTOR\E"\s*\)""").containsMatchIn(abiHeader.readText()),
            "MOQ5_ABI_METHODS must carry X($NAME, \"$DESCRIPTOR\")",
        )
    }

    @Test
    fun `the bound bytecode carries the row with the transport parameters`() {
        val m = NativeBridge::class.java.declaredMethods.filter { it.name == NAME }
        assertEquals(1, m.size, "exactly one $NAME on NativeBridge; found ${m.map { it.toGenericString() }}")
        val method = m.single()
        assertEquals(
            listOf(String::class.java, Integer.TYPE, Integer.TYPE, IntArray::class.java, java.lang.Long.TYPE, Integer.TYPE, String::class.java, LongArray::class.java),
            method.parameterTypes.toList(),
        )
        assertEquals(Integer.TYPE, method.returnType)
        val mods = method.modifiers
        assertTrue(Modifier.isPublic(mods) && Modifier.isFinal(mods) && Modifier.isNative(mods) && !Modifier.isStatic(mods))
    }

    @Test
    fun `the shipped keep rule carries the transport row so it survives R8`() {
        assertTrue(keepRules.isFile, "setup: ${keepRules.absolutePath}")
        assertTrue(
            keepRules.readLines().any { it.trim() == "public final native int $NAME(java.lang.String, int, int, int[], long, int, java.lang.String, long[]);" },
            "consumer-rules.pro must keep $NAME by exact name and type",
        )
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE)
            .find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        // The transport row moved the epoch to 8; later rows move it further.
        assertTrue(declared != null && declared >= EPOCH_WITH_ROW, "the authority's epoch is $declared, needs $EPOCH_WITH_ROW or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }
}
