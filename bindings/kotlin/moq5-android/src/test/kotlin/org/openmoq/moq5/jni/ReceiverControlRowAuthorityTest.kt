package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The receiver CONTROL rows: subscribe a track (start mode ordinal, a
 * has-priority flag and the priority), unsubscribe it, and read its state
 * into a one-slot block. Each takes the root, the receiver child and the
 * track's borrowed token; the endpoint's own `moq_result_t` crosses exactly
 * and a registry refusal is the registry's.
 */
internal class ReceiverControlRowAuthorityTest {
    private companion object {
        val ROWS = listOf(
            "nativeTrackSubscribe" to "(JJJIZI)I",
            "nativeTrackUnsubscribe" to "(JJJ)I",
            "nativeTrackState" to "(JJJ[J)I",
        )
        const val EPOCH_WITH_ROWS = 12
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
        val preflight = File("../tools/check-jni-endpoint-preflight.sh")
    }

    @Test
    fun `the authority table carries the three control rows with their descriptors`() {
        val table = abiHeader.readText()
        for ((name, desc) in ROWS) {
            assertTrue(Regex("""X\(\s*$name\s*,\s*"\Q$desc\E"\s*\)""").containsMatchIn(table), "MOQ5_ABI_METHODS must carry X($name, \"$desc\")")
        }
    }

    @Test
    fun `the bound bytecode carries the rows as instance public final native methods`() {
        val subscribe = NativeBridge::class.java.declaredMethods.single { it.name == "nativeTrackSubscribe" }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, java.lang.Long.TYPE, Integer.TYPE, java.lang.Boolean.TYPE, Integer.TYPE), subscribe.parameterTypes.toList())
        val unsubscribe = NativeBridge::class.java.declaredMethods.single { it.name == "nativeTrackUnsubscribe" }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, java.lang.Long.TYPE), unsubscribe.parameterTypes.toList())
        val state = NativeBridge::class.java.declaredMethods.single { it.name == "nativeTrackState" }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, java.lang.Long.TYPE, LongArray::class.java), state.parameterTypes.toList())
        for (m in listOf(subscribe, unsubscribe, state)) {
            assertEquals(Integer.TYPE, m.returnType)
            assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers))
        }
    }

    @Test
    fun `the shipped keep rules carry the rows so they survive R8`() {
        val lines = keepRules.readLines().map { it.trim() }
        assertTrue("public final native int nativeTrackSubscribe(long, long, long, int, boolean, int);" in lines)
        assertTrue("public final native int nativeTrackUnsubscribe(long, long, long);" in lines)
        assertTrue("public final native int nativeTrackState(long, long, long, long[]);" in lines)
    }

    @Test
    fun `the ABI epoch advanced with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        assertTrue(declared != null && declared >= EPOCH_WITH_ROWS, "the authority's epoch is $declared, needs $EPOCH_WITH_ROWS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the preflight names the control substrate the rows execute`() {
        val text = preflight.readText()
        for (sym in listOf("moq_media_receiver_track_subscribe_cfg_init", "moq_media_receiver_subscribe_track", "moq_media_receiver_unsubscribe_track", "moq_media_receiver_track_state")) {
            assertTrue(Regex("""RX_SET='[^']*\b$sym\b[^']*'""").containsMatchIn(text), "RX_SET must name $sym so both Android images prove it by identity")
        }
    }
}
