package org.openmoq.moq5.jni

import java.io.File
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The receiver SUBSTRATE: before any receiver row can be executed by a
 * deterministic host test, the one-DSO bridge must define the media
 * receiver's entry points. Today the host image compiles only the endpoint
 * out of the service tier, and the Android image links the service archive
 * without anything pulling the receiver object in -- so this row names the
 * prerequisite by symbol, exactly as the endpoint preflight names its six.
 */
internal class ReceiverSubstrateTest {
    private companion object {
        val ENTRY_POINTS = listOf(
            "moq_media_receiver_cfg_init",
            "moq_media_receiver_attach",
            "moq_media_receiver_destroy",
            "moq_media_receiver_wait",
            "moq_media_receiver_poll_track",
            "moq_media_track_desc_get",
            "moq_media_receiver_is_closed",
            "moq_media_receiver_is_fatal",
            "moq_media_receiver_fatal_code",
        )
    }

    @Test
    fun `the host bridge image defines the receiver entry points once each`() {
        val dso = File(BridgeDsos.path("moq5_jni"))
        val nm = listOf("/usr/bin/nm", "/opt/homebrew/opt/llvm/bin/llvm-nm").map(::File).firstOrNull { it.canExecute() }
        assertTrue(nm != null, "setup: no nm on this host")
        // The image exports only JNI_OnLoad; the substrate is judged on every
        // DEFINED symbol, local ones included, exactly as the endpoint preflight does.
        val proc = ProcessBuilder(nm!!.path, "--defined-only", dso.path).redirectErrorStream(true).start()
        val symbols = proc.inputStream.bufferedReader().readLines()
        assertTrue(proc.waitFor() == 0, "setup: nm failed on ${dso.path}")
        val defined = symbols.mapNotNull { line -> line.trim().split(Regex("\\s+")).lastOrNull()?.removePrefix("_") }
        val missing = ENTRY_POINTS.filter { ep -> defined.count { it == ep } != 1 }
        assertTrue(missing.isEmpty(), "the host bridge image must define each receiver entry point exactly once; missing or duplicated: $missing")
    }
}
