package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The add-track row acquires the caller's output storage before anything is
 * owed, so an acquisition failure costs nothing.
 *
 * `GetLongArrayElements` may answer NULL with or without a Java exception
 * pending. Neither can be provoked on demand from a healthy VM, so both are
 * INJECTED, once, at load, in the host image only -- the pending arm by
 * provoking a genuine runtime exception rather than throwing a synthetic one.
 * Everything the rows then observe is real: the array, the call count, the
 * next add.
 */
internal class SenderOutputAcquireTest {
    private fun ok(mode: String, what: String) {
        val env = TrustedChild.probeArmed +
            (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING) +
            ("MOQ5_JNI_HOST_TEST_FAIL_ACQUIRE" to mode)
        TrustedChild.assertOk(EndpointAbi.child("senderoutputacquire", EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `an acquisition that answers NULL without an exception is the allocation code, and nothing was added`() =
        ok(
            "fail-acquire-bare-v1",
            "a NULL acquisition with no pending exception answers MOQ_ERR_NOMEM, leaves the caller's array " +
                "untouched, reaches the C add zero times, and the next add succeeds under the same name",
        )

    @Test
    fun `an acquisition that answers NULL with an exception pending keeps that exception, and nothing was added`() =
        ok(
            "fail-acquire-pending-v1",
            "a NULL acquisition with a pending runtime exception hands that exception to the caller unchanged, " +
                "leaves the caller's array untouched, reaches the C add zero times, and the next add succeeds",
        )
}
