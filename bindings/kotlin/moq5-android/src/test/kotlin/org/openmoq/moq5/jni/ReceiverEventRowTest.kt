package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * Discovery through the real one-DSO bridge: a receiver attached to a LIVE
 * scripted root has a catalog ingested by a host-only fixture native (the
 * service's own `moq_media_receiver_test_ingest` seam), and the REAL
 * `moq_media_receiver_poll_track` answers TRACK_ADDED with a real
 * description, then CATALOG_READY, then empty. Scenarios in [EndpointChildMain].
 */
internal class ReceiverEventRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `a real catalog yields ADDED with a borrowed track token and a real description, then CATALOG_READY, then empty`() =
        ok("receiverdiscovery", "poll -> DONE before any catalog; ingest {a: loc/video, b: loc/audio}; poll -> ADDED(a) with a kind-3 token under the receiver and the exact bytes; ADDED(b); CATALOG_READY (token 0); DONE")

    @Test
    fun `track tokens are borrowed children of their receiver and die with it`() =
        ok("receivertracktokens", "describe under a foreign receiver -> REG_OWNER; poll/describe of the receiver under the wrong root -> REG_OWNER; after receiver close, poll and describe -> REG_STALE for receiver and track tokens; the root still stops")

    @Test
    fun `malformed carriers are refused before any poll or describe, and foreign tokens are the registry's refusal`() =
        ok("receivereventmalformed", "null/short poll scalars -> INVAL untouched and nothing consumed; null/short describe scalars -> null (+INVAL in slot 0); unknown track -> REG_STALE; the receiver token as a track -> REG_KIND")

    @Test
    fun `a refused track-token insertion after a consumed event is a bridge failure, never a track, and the receiver still retires`() =
        ok("receivertrackinsertfault", "with the next child insertion refused: poll -> REG_CAPACITY, no token published; the next poll mints a token; close, stop, destroy succeed")

    @Test
    fun `the backend maps empty, event, closed and refused answers without folding`() =
        ok("backendreceiverevents", "Empty for DONE; Event with the same token for ADDED; a stale receiver token -> Bridge(REG_STALE); never a MoqResultCode")
}
