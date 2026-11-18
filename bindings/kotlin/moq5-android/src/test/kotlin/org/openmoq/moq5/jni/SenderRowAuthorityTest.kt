package org.openmoq.moq5.jni

import java.io.File
import java.lang.reflect.Modifier
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The eleven SENDER rows: the sender an OWNED child of the root, its tracks
 * BORROWED children of the sender; every row in the authority with its
 * descriptor, bound as an instance native method, kept by the shipped rules,
 * at ABI epoch 15 or later on both sides, with the sender substrate named by the
 * preflight.
 */
internal class SenderRowAuthorityTest {
    private companion object {
        const val EPOCH_WITH_ROWS = 15
        val ROWS =
            listOf(
                "nativeSenderAttach" to "(J[[B[B[J[J)I",
                "nativeSenderClose" to "(JJ)I",
                "nativeSenderAddTrack" to "(JJ[[B[J[J)I",
                "nativeSenderRemoveTrack" to "(JJJ)I",
                "nativeSenderWrite" to "(JJJ[B[B[J)I",
                "nativeSenderEndTrack" to "(JJJ)I",
                "nativeSenderConvertToVod" to "(JJ[J[J)I",
                "nativeSenderComplete" to "(JJ)I",
                "nativeSenderProbe" to "(JJ[J)I",
                "nativeSenderTrackDemand" to "(JJJ[J)I",
                "nativeSenderStats" to "(JJ[J)I",
            )
        val KEEP =
            listOf(
                "public final native int nativeSenderAttach(long, byte[][], byte[], long[], long[]);",
                "public final native int nativeSenderClose(long, long);",
                "public final native int nativeSenderAddTrack(long, long, byte[][], long[], long[]);",
                "public final native int nativeSenderRemoveTrack(long, long, long);",
                "public final native int nativeSenderWrite(long, long, long, byte[], byte[], long[]);",
                "public final native int nativeSenderEndTrack(long, long, long);",
                "public final native int nativeSenderConvertToVod(long, long, long[], long[]);",
                "public final native int nativeSenderComplete(long, long);",
                "public final native int nativeSenderProbe(long, long, long[]);",
                "public final native int nativeSenderTrackDemand(long, long, long, long[]);",
                "public final native int nativeSenderStats(long, long, long[]);",
            )
        val abiHeader = File("../jni/moq5_abi.h")
        val keepRules = File("consumer-rules.pro")
        val preflight = File("../tools/check-jni-endpoint-preflight.sh")
    }

    @Test
    fun `the authority table carries the eleven sender rows with their descriptors, once each`() {
        val text = abiHeader.readText()
        for ((row, desc) in ROWS) {
            assertEquals(1, Regex("""X\(\s*$row\s*,\s*"\Q$desc\E"\s*\)""").findAll(text).count(), "MOQ5_ABI_METHODS must carry X($row, \"$desc\") exactly once")
        }
        assertEquals(11, ROWS.size)
    }

    @Test
    fun `the bound bytecode carries every row as an instance public final native method`() {
        for ((row, _) in ROWS) {
            val m = NativeBridge::class.java.declaredMethods.single { it.name == row }
            assertEquals(Integer.TYPE, m.returnType, row)
            assertTrue(Modifier.isPublic(m.modifiers) && Modifier.isFinal(m.modifiers) && Modifier.isNative(m.modifiers) && !Modifier.isStatic(m.modifiers), row)
        }
        val write = NativeBridge::class.java.declaredMethods.single { it.name == "nativeSenderWrite" }
        assertEquals(listOf(java.lang.Long.TYPE, java.lang.Long.TYPE, java.lang.Long.TYPE, ByteArray::class.java, ByteArray::class.java, LongArray::class.java), write.parameterTypes.toList())
    }

    @Test
    fun `the shipped keep rules carry every row`() {
        val lines = keepRules.readLines().map { it.trim() }
        for (k in KEEP) assertTrue(k in lines, k)
    }

    @Test
    fun `the ABI epoch advanced to 15 with the table on both sides`() {
        val declared = Regex("""^#define MOQ5_ABI_EPOCH (\d+)$""", RegexOption.MULTILINE).find(abiHeader.readText())?.groupValues?.get(1)?.toInt()
        // The sender rows moved the epoch to 15; later rows move it further.
        assertTrue(declared != null && declared >= EPOCH_WITH_ROWS, "the authority's epoch is $declared, needs $EPOCH_WITH_ROWS or later")
        assertEquals(declared, NativeBridge.ABI_EPOCH, "the Kotlin side's epoch")
    }

    @Test
    fun `the preflight names the sender substrate and never the C wait`() {
        val text = preflight.readText()
        val set = Regex("""TX_SET='([^']*)'""").find(text)?.groupValues?.get(1) ?: ""
        for (sym in listOf("moq_media_sender_attach", "moq_media_sender_write", "moq_media_sender_end_track", "moq_media_sender_convert_to_vod", "moq_media_sender_complete", "moq_media_sender_get_stats", "moq_rcbuf_create", "moq_rcbuf_decref")) {
            assertTrue(Regex("""\b$sym\b""").containsMatchIn(set), sym)
        }
        assertTrue(!Regex("""\bmoq_media_sender_wait\b""").containsMatchIn(set), "moq_media_sender_wait is not part of the Android substrate")
    }
}
