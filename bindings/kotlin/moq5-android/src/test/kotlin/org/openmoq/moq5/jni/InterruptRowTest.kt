package org.openmoq.moq5.jni

import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The interrupt-latch row through the real one-DSO bridge, each row a child
 * JVM so a native fault is attributable. Scenarios in [EndpointChildMain].
 */
internal class InterruptRowTest {
    @Test
    fun `the latch row sets, beats a raised level, clears, and answers from any thread`() {
        val out = EndpointAbi.child("setinterrupted", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(out.contains("RESULT ok"), "output was:\n" + out.joinToString("\n"))
    }

    @Test
    fun `a stale token is a registry refusal as an Int, never a throw or a MoqResultCode`() {
        val out = EndpointAbi.child("setinterruptedstale", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(out.contains("RESULT ok"), "output was:\n" + out.joinToString("\n"))
    }

    @Test
    fun `the backend maps the row to Ok or Bridge and nothing else`() {
        val out = EndpointAbi.child("backendsetinterrupted", EndpointAbi.SCRIPTED_TOKEN)
        assertTrue(out.contains("RESULT ok"), "output was:\n" + out.joinToString("\n"))
    }
}
