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
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROWS, "the authority's epoch is $declared, needs $EPOCH_WITH_ROWS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }
}
