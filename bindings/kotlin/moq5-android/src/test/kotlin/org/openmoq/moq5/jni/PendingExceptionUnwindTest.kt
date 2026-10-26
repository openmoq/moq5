package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * R11: a row whose result allocation fails must unwind legally, in BOTH answers
 * that failure may give.
 *
 * A JNI function that can return NULL and throw is permitted to report failure
 * with NULL alone, so the two are different answers. With an exception pending
 * only a restricted set of JNI functions may be called and a scalar writer is
 * not among them, so the caller's block must be left untouched and the
 * exception is the answer. With nothing pending the block is the caller's only
 * channel and the refusal must be published there -- losing it would report
 * `MOQ_OK` out of a zero-initialised block.
 *
 * The allocation failure is injected one-shot by the host recorder immediately
 * before the row it is meant for, so no setup row consumes it; a healthy VM
 * will not allocate-fail on demand. What the rows observe afterwards is real:
 * the exception or its absence, the caller's block, the object-cleanup counter,
 * and the exact payload the retry delivers.
 */
internal class PendingExceptionUnwindTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `a describe whose allocation throws leaves every caller slot untouched`() =
        ok(
            "describeallocpending",
            "the runtime's exception reaches the caller unchanged, no scalar is written while it is pending, " +
                "no object is released, and the next describe succeeds",
        )

    @Test
    fun `a describe whose allocation answers NULL alone publishes the refusal`() =
        ok(
            "describeallocbare",
            "with nothing pending the refusal is published in slot 0 and nowhere else, no exception is raised, " +
                "no object is released, and the next describe succeeds",
        )

    @Test
    fun `an object poll whose allocation throws leaves every caller slot untouched and still releases the object`() =
        ok(
            "objectallocpending",
            "the runtime's exception reaches the caller unchanged, no scalar is written while it is pending, " +
                "exactly one object is released, and the retry delivers the declared second object",
        )

    @Test
    fun `an object poll whose allocation answers NULL alone publishes the refusal and still releases the object`() =
        ok(
            "objectallocbare",
            "with nothing pending the refusal is published in slot 0 and nowhere else, exactly one object is " +
                "released, and the retry delivers the declared second object",
        )

    @Test
    fun `an object poll that fails after its result array exists releases the object just the same`() =
        ok(
            "objectallocpartial",
            "a byte field that cannot be allocated leaves a partially built result, and it takes the same " +
                "cleanup path: the refusal is published, exactly one object is released, and the retry " +
                "delivers the declared second object",
        )
}
