package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The receiver OBJECT row: one `moq_media_receiver_poll_object` attempt on
 * the authority thread, the bytes copied out as byte arrays (payload,
 * fragment, packed samples) with the scalars in a fixed block, and the C
 * object cleaned exactly once before the row returns. Null with the code in
 * the block's first slot for every non-object answer.
 */
internal class ReceiverObjectRowAuthorityTest {
    private companion object {
        const val ROW = "nativeReceiverPollObject"
        const val DESCRIPTOR = "(JJ[J)[[B"
        const val EPOCH_WITH_ROW = 11
        const val EPOCH_WITH_IDENTITY_SCALARS = 17
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
        val preflight = File("../tools/check-jni-endpoint-preflight.sh")
    }

    @Test
    fun `the authority table carries the object row with its descriptor`() {
        assertTrue(Regex("""X\(\s*$ROW\s*,\s*"\Q$DESCRIPTOR\E"\s*\)""").containsMatchIn(abiHeader.readText()), "MOQ5_ABI_METHODS must carry X($ROW, \"$DESCRIPTOR\")")
    }

    @Test
    fun `the bound bytecode carries the row as an instance public final native method`() {
        val m = NativeBridge::class.java.declaredMethods.single { it.name == ROW }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, LongArray::class.java), m.parameterTypes.toList())
        assertEquals(Array<ByteArray>::class.java, m.returnType)
        assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers))
    }

    @Test
    fun `the shipped keep rules carry the row so it survives R8`() {
        assertTrue("public final native byte[][] $ROW(long, long, long[]);" in keepRules.readLines().map { it.trim() })
    }

    @Test
    fun `the object carrier's identity scalars moved the epoch past the last 17-scalar epoch`() {
        // The scalar block behind this row's descriptor is part of what the
        // epoch names: a native side at epoch 16 fills 17 scalars and can never
        // deliver identity, so the Kotlin side that reads 23 must refuse it at
        // the handshake rather than discover the shorter block on a poll.
        assertEquals(23, ReceiverObjectScalars.COUNT)
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_IDENTITY_SCALARS, "the authority's epoch is $declared, needs $EPOCH_WITH_IDENTITY_SCALARS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROW, "the authority's epoch is $declared, needs $EPOCH_WITH_ROW or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the preflight names the object substrate the row executes`() {
        val text = preflight.readText()
        for (sym in listOf("moq_media_receiver_poll_object", "moq_media_object_cleanup")) {
            assertTrue(Regex("""RX_SET='[^']*\b$sym\b[^']*'""").containsMatchIn(text), "RX_SET must name $sym so both Android images prove it by identity")
        }
    }
}
