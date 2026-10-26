package org.openmoq.moq5.jni

import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.CodecSignaling
import org.openmoq.moq5.MediaBytes
import kotlin.system.exitProcess

/**
 * A child JVM that calls the PUBLIC codec object twice and reports what it saw
 * and what the process-wide owner decided, one line per fact. Its library path
 * is chosen by the parent row; it never selects a library itself.
 */
internal object CodecOwnerChildMain {
    private fun attempt(): String =
        try {
            val r = CodecSignaling.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(CodecVectors.AVC_ANNEXB))
            "RESULT " + r.bytes().joinToString("") { "%02x".format(it) }
        } catch (t: Throwable) {
            "THROWN ${t.javaClass.name} $t"
        }

    @JvmStatic
    fun main(args: Array<String>) {
        val first = attempt()
        val afterFirst = NativeBridgeOwner.outcome
        val second = attempt()
        val afterSecond = NativeBridgeOwner.outcome
        println("FIRST $first")
        println("SECOND $second")
        println("OUTCOME $afterFirst")
        println("OUTCOME_STICKY ${afterFirst != null && afterFirst === afterSecond}")
        System.out.flush()
        exitProcess(0)
    }
}
