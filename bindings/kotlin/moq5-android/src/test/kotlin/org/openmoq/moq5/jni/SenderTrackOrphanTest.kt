package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * R12: a refused track-token mint must not leave a track behind.
 *
 * The row adds the track at the C first and mints its token second, and its
 * rollback has a precondition the addition does not: `remove_track` is refused
 * while the endpoint's interrupt latch is set, `add_track` is not. With the
 * latch set and the mint refused, the rollback is refused too and its answer is
 * discarded, so the sender keeps a track under a name no token can address.
 *
 * The child collects every consequence, so one run names all of them rather
 * than stopping at the first.
 */
internal class SenderTrackOrphanTest {

    @Test
    fun `a refused track-token mint leaves no track the C still holds and nothing can name`() {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(
            EndpointAbi.child("sendertrackorphan", EndpointAbi.SCRIPTED_TOKEN, env),
            "a refused mint must take its C addition back: the name stays free, the registry consumes " +
                "nothing, and no unnameable track survives",
        )
    }
}
