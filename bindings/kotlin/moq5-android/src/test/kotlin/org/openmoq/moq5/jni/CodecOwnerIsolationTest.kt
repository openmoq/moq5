package org.openmoq.moq5.jni

import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The PUBLIC codec object's bridge gate, each arm in its own child JVM so the
 * process-wide owner's single decision is observed from a clean process: an
 * unresolvable packaged library is the public initialization failure, sticky
 * across calls; a resolvable one is a Ready owner decision reached through the
 * codec entry itself.
 */
internal class CodecOwnerIsolationTest {
    private fun run(libraryPath: String?): Map<String, String> {
        val args = if (libraryPath == null) emptyList() else listOf("-Djava.library.path=$libraryPath")
        val lines = EndpointAbi.child("codec-owner", null, jvmArgs = args, mainClass = CodecOwnerChildMain::class.java.name)
        return lines.mapNotNull { l -> l.indexOf(' ').takeIf { it > 0 }?.let { l.substring(0, it) to l.substring(it + 1) } }.toMap()
    }

    @Test
    fun `an unresolvable packaged library is the sticky public initialization failure`() {
        val empty = Files.createTempDirectory("moq5-no-bridge").toFile()
        try {
            val out = run(empty.absolutePath)
            val first = out["FIRST"].orEmpty()
            assertTrue(first.startsWith("THROWN org.openmoq.moq5.NativeBridgeInitializationException"), first)
            assertTrue(first.contains("LoadFailed(libraryName=moq5_jni"), first)
            assertEquals(first, out["SECOND"], "the same decision on the second call")
            assertTrue(out["OUTCOME"].orEmpty().startsWith("LoadFailed("), "the owner decided: ${out["OUTCOME"]}")
            assertEquals("true", out["OUTCOME_STICKY"], "the owner's decision object is the same after both calls")
        } finally {
            empty.delete()
        }
    }

    @Test
    fun `a resolvable packaged library is a Ready owner decision reached through the codec entry`() {
        val out = run(null)
        val avcc = "RESULT " + CodecVectors.AVCC.joinToString("") { "%02x".format(it) }
        assertEquals(avcc, out["FIRST"], "the public entry built the reviewed record through the owner")
        assertEquals(avcc, out["SECOND"])
        assertTrue(out["OUTCOME"].orEmpty().startsWith("Ready("), "the owner decided: ${out["OUTCOME"]}")
        assertEquals("true", out["OUTCOME_STICKY"])
    }
}
