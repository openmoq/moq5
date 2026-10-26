package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The object row through the real one-DSO bridge: a receiver on a LIVE
 * scripted root has a catalog and then media objects fed through the
 * service's host-only seams, and the REAL `moq_media_receiver_poll_object`
 * answers. The recorder counts every `moq_media_object_cleanup` and poisons
 * the object's bytes inside it, so a row can prove exactly-once cleanup and
 * copy-before-cleanup. A session-less receiver needs two explicit test-only
 * preconditions before an object is admitted: the transport draft the pump
 * would read from a live session (the track descriptions derive from it, so
 * it precedes the catalog), and the track marked DESIRED
 * (`receiver_on_media_object` drops objects for an undesired track before
 * parse). Each has its own non-vacuity row below. Track tokens are minted
 * on the bridge's first sight of a handle, from either row. Scenarios in
 * [EndpointChildMain].
 */
internal class ReceiverObjectRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `without the transport-draft precondition the real intake parses nothing, and the drop is the track's coalesced PARSE_DROP`() =
        ok("receiverobjectnoversion", "ingest without the draft; ADDED still; mark desired; feed -> poll DONE, no cleanup; the next track poll is PARSE_DROP for that track")

    @Test
    fun `an object for an undesired track is dropped at the real intake, and the explicit desire precondition is what admits one`() =
        ok("receiverobjectundesired", "discovery-only attach; feed an object WITHOUT the precondition -> poll DONE, no cleanup counted; mark the track desired through the helper; feed again -> the object delivers")

    @Test
    fun `a LOC object is copied out as RAW geometry with its times, and the C object is cleaned exactly once`() =
        ok("receiverobjectloc", "poll -> DONE before any object; ingest one LOC object; poll -> OK with the track's borrowed token, packaging RAW, payload bytes exact, fragment empty, 0 samples, capture/decode/presentation as fed, cleanup count 1; poll -> DONE")

    @Test
    fun `a CMAF object keeps the full fragment with its mdat range and samples, and its payload is empty`() =
        ok("receiverobjectcmaf", "ingest one CMAF fragment; poll -> packaging CMAF, fragment bytes exact, payload empty, mdat range inside the fragment, sample count and packed samples as the parser produced; cleanup count 1")

    @Test
    fun `malformed carriers refuse before any native poll`() =
        ok("receiverobjectmalformed", "null scalars -> null; short scalars -> null with INVAL in slot 0, the rest untouched, and the queued object still delivers afterwards (nothing was consumed)")

    @Test
    fun `the endpoint latch beats a queued object and consumes nothing, clearing it delivers`() =
        ok("receiverobjectinterrupted", "set the latch; poll -> null with MOQ_ERR_INTERRUPTED; clear; poll -> the same object; cleanup count 1")

    @Test
    fun `an object polled before its track's ADDED is drained delivers with a first-sight token that the later track poll reuses`() =
        ok("receiverobjectfirstsight", "ingest catalog + object WITHOUT draining track events; poll object -> the object with a kind-3 token minted on first sight, cleanup count 1; the later track poll's ADDED carries the SAME token")

    @Test
    fun `the wire identity crosses the carrier from the real intake, ids bit-for-bit and the datagram without a subgroup`() =
        ok("receiverobjectidentity", "ingest three objects with subgroup/priority and one datagram; poll each -> HAS_IDENTITY set, group/subgroup/object as fed (one above Long.MAX_VALUE), priority as fed, HAS_SUBGROUP clear only for the datagram; cleanup count 4")

    @Test
    fun `wrong-owner and stale receiver tokens stay registry refusals on the object path`() =
        ok("receiverobjectownership", "poll under the wrong root -> REG_OWNER; after receiver close -> REG_STALE; the roots still stop")
}
