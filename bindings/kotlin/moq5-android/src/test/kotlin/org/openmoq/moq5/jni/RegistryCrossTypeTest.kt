package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * R9 (`chats/libmoq/1530_android_overall_review.md`): the private registry
 * distinguishes LIFETIME roles -- root, owned child, borrowed leaf -- but not
 * the CONCRETE native type behind an owned child. A receiver child and a
 * sender child of one endpoint root carry the same kind and the same owner,
 * so a consumer that validates kind and owner alone accepts either.
 *
 * The row runs through the real one-DSO bridge in the isolated child process
 * ([EndpointChildMain]), against real `moq_media_receiver_t` and
 * `moq_media_sender_t` children of a live root, at same-root live-generation
 * tokens. Both directions and both boundaries -- retirement and acquisition
 * -- are covered, and their failures are collected so one run attributes all
 * of them. Matching-type, foreign-owner, borrowed-leaf, never-issued and
 * stale controls run beside them; wrong-thread confinement stays where it is
 * already proven, in the row authority suites.
 *
 * Safety: the close rows run each entry's OWN registered cleanup, so a
 * cross-type close destroys the wrong object without ever reinterpreting a
 * pointer; the acquisition rows are driven with an INVALID track token, so
 * the row refuses at its second acquisition and returns before the mis-typed
 * wrapper is dereferenced. Nothing casts and dereferences an incompatible
 * object, and no crash is used as an oracle.
 *
 * This is the RED for R9. It is expected to fail until the registry gains a
 * concrete-type check on acquisition AND retirement.
 */
internal class RegistryCrossTypeTest {
    @Test
    fun `a sender token and a receiver token of one root are not interchangeable at acquisition or retirement`() {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(
            EndpointAbi.child("registrycrosstype", EndpointAbi.SCRIPTED_TOKEN, env),
            "cross-type: sender close of a receiver token and receiver close of a sender token are refused by concrete type, " +
                "each child survives, and a sender/receiver row refuses a cross-type child before its track acquisition",
        )
    }
}
