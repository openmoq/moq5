package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * Track-event kinds 8-12 and the FAILED state through the real one-DSO
 * bridge: one scripted answer at the `poll_track` / `track_state` boundary on
 * a real track handle, then the product's own slots, coverage and decoding.
 * Scenarios in [TrackEventKindScenarios].
 *
 * HOST fixture: the root is the scripted target, and the fill and failure
 * events are scripted at the C API boundary, not produced by a live peer.
 */
internal class TrackEventKindRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `every kind the header defines decodes with its exact payload, and values outside the C sets stay the bridge's refusal`() =
        ok("receiverlocalkinds", "LOCAL_FAILURE, UPDATE_LOCAL_FAILURE and DELIVERY_LOCAL_FAILURE carry their signed results; LIVE has no child ID, FILL a zero one; FILL_TERMINAL keeps a zero ID, a STREAM code of zero, full-width IDs and its loss summary; FILL_CREATED its ID; kind 13, reason 9, domain 4, loss bit 8 and origin 3 are refused; real discovery continues in order")

    @Test
    fun `a field is published only when the stamp wholly covers it, and an unobserved field is never a zero`() =
        ok("receiverkindstamps", "a stamp inside local_result publishes no HAS bit; the fill fields drop out one by one as the stamp shortens; the loss block is all or nothing; a FILL origin without its child ID, then no origin")

    @Test
    fun `the FAILED track state crosses as 6 and reads as Failed`() =
        ok("receiverfailedstate", "one scripted FAILED read, then the real DISCOVERED")

    @Test
    fun `on a draft-22 receiver a fill terminal is an event, the parent stays present, and its objects on both sides are delivered`() =
        ok("publicfilltermd22", "public API over the real JNI backend: both objects in order, Added(a), FillTerminal(a, 0, PeerCancelledOrReset, Stream, 0, None), Added(b), CatalogReady; a stays Present and not Failed; no quarantine")
}
