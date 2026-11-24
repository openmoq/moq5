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
    fun `the replacement links are one block, published only when the stamp covers replaced_by, and decoded on the kinds that carry them`() =
        ok("receiverreplacementstamps", "a whole stamp publishes HAS_REPLACEMENT with the successor's token; a stamp inside replaced_by, or ending before it, publishes neither slot; REMOVED decodes replacedBy, ADDED decodes replaces, a NULL link is null; the real CATALOG_READY follows")

    @Test
    fun `through the public API a replacement is Removed(replaced) then Added(replaces), the tracks are linked, and a command on the old track reaches the new one natively`() =
        ok("publicreplacement", "public API over the real JNI backend: Added(a), Added(other), CatalogReady, Removed(a, replaced), Added(b, replaces = a); a.replacedBy === b; subscribe(a) -> b Pending natively; trackState(a) -> Removed; unsubscribe(a) -> b Discovered")

    @Test
    fun `the FAILED track state crosses as 6 and reads as Failed`() =
        ok("receiverfailedstate", "one scripted FAILED read, then the real DISCOVERED")

    @Test
    fun `on a draft-22 receiver a fill terminal is an event, the parent stays present, and its objects on both sides are delivered`() =
        ok("publicfilltermd22", "public API over the real JNI backend: both objects in order, Added(a), FillTerminal(a, 0, PeerCancelledOrReset, Stream, 0, None), Added(b), CatalogReady; a stays Present and not Failed; no quarantine")
}
