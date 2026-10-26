package org.openmoq.moq5.jni

import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * The drain row through the real one-DSO bridge, each row a child JVM so a
 * native fault is attributable. Scenarios in [EndpointChildMain]. The host
 * facade models a LIVE endpoint and reports a caller-chosen transport drain
 * state; it never decides the outcome, which is the product's own loop.
 */
internal class DrainRowTest {
    private fun ok(scenario: String, mode: String? = null) {
        val env = if (mode == null) emptyMap() else mapOf(EndpointAbi.MODE to mode)
        val out = EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env)
        assertTrue(out.contains("RESULT ok"), "output was:\n" + out.joinToString("\n"))
    }

    @Test
    fun `a pending transport times out as MOQ_DONE, a negative timeout is refused, the latch answers first, and the row is any-thread`() =
        ok("drain", EndpointAbi.MODE_DRAIN_PENDING)

    @Test
    fun `a stopped endpoint answers MOQ_ERR_CLOSED and a stale token the registry's refusal as an Int`() =
        ok("drainclosed", EndpointAbi.MODE_DRAIN_PENDING)

    @Test
    fun `an idle transport answers MOQ_OK`() = ok("draindone", EndpointAbi.MODE_DRAIN_DONE)

    @Test
    fun `a backend that cannot prove a flush answers MOQ_ERR_UNSUPPORTED`() = ok("drainunsupported", EndpointAbi.MODE_DRAIN_UNSUPPORTED)

    @Test
    fun `the backend maps every documented answer and folds nothing`() = ok("backenddrain", EndpointAbi.MODE_DRAIN_PENDING)
}
