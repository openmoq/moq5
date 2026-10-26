package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The receiver RECORD rows: one `poll_sap` / `poll_media_timeline` attempt
 * into a fixed scalar block that also carries the family's deferral
 * counter (`moq_media_receiver_get_stats`), read in the same JNI row under
 * the same registry pin and on the authority thread, strictly after the
 * poll. The two C calls take the receiver's mutex separately, so this is
 * not one receiver critical section; the counter only grows, so it is never
 * older than the answer it accompanies.
 */
internal class ReceiverTimelineRowAuthorityTest {
    private companion object {
        val ROWS = listOf("nativeReceiverPollSap" to "(JJ[J)I", "nativeReceiverPollMediaTimeline" to "(JJ[J)I")
        const val EPOCH_WITH_ROWS = 13
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
        val preflight = File("../tools/check-jni-endpoint-preflight.sh")
    }

    @Test
    fun `the authority table carries both record rows with their descriptors`() {
        val table = abiHeader.readText()
        for ((name, desc) in ROWS) assertTrue(Regex("""X\(\s*$name\s*,\s*"\Q$desc\E"\s*\)""").containsMatchIn(table), "MOQ5_ABI_METHODS must carry X($name, \"$desc\")")
    }

    @Test
    fun `the bound bytecode carries both rows as instance public final native methods`() {
        for ((name, _) in ROWS) {
            val m = NativeBridge::class.java.declaredMethods.single { it.name == name }
            assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, LongArray::class.java), m.parameterTypes.toList())
            assertEquals(Integer.TYPE, m.returnType)
            assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers))
        }
    }

    @Test
    fun `the shipped keep rules carry both rows`() {
        val lines = keepRules.readLines().map { it.trim() }
        assertTrue("public final native int nativeReceiverPollSap(long, long, long[]);" in lines)
        assertTrue("public final native int nativeReceiverPollMediaTimeline(long, long, long[]);" in lines)
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROWS, "the authority's epoch is $declared, needs $EPOCH_WITH_ROWS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the preflight names the record substrate the rows execute`() {
        val text = preflight.readText()
        for (sym in listOf("moq_media_receiver_poll_sap", "moq_media_receiver_poll_media_timeline", "moq_media_receiver_get_stats")) {
            assertTrue(Regex("""RX_SET='[^']*\b$sym\b[^']*'""").containsMatchIn(text), "RX_SET must name $sym")
        }
    }
}
