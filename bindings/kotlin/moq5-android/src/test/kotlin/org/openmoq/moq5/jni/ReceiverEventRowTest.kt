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
    fun `describe reads the locked copy -- one copy per describe, a VOD conversion is reported, and a refused copy is the result with the pins released`() =
        ok("receiverdescribecopy", "ingest a live; describe -> one copy, live, no duration; generation 2 converts a to VOD -> TRACK_UPDATED; describe -> a second copy, VOD 60000 ms; arm one refusal -> describe answers INVAL in slot 0 only; the next describe answers; close, stop and destroy return")

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
    fun `a critical change replaces the track -- REMOVED names the successor's token, the ADDED that follows names the predecessor's, and commands on the old track reach the new one`() =
        ok("receiverreplacement", "MSF-02 base {video, other}; a codec update of video -> REMOVED(a) with HAS_REPLACEMENT and REPLACED_BY a fresh kind-3 token b, REPLACES 0; ADDED(b) with REPLACES a, generation 0, the new codec; a's description untouched; a bitrate update of other -> UPDATED with CONFIG_GENERATION 1 and describe generation 1; subscribe(a) -> b PENDING, a's state WRONG_STATE, unsubscribe(a) -> b DISCOVERED")

    @Test
    fun `a refused token mint for the successor after the REMOVED was consumed is the registry's code, and a bridge failure through the backend`() =
        ok("receiverreplacementmintfault", "next child insertion refused at the REMOVED -> REG_CAPACITY, nothing published; the ADDED(b) that follows mints and names a; through JniEndpointBackend the same refusal is Bridge(receiver-poll-track, REG_CAPACITY)")

    @Test
    fun `the backend decodes the replacement pair as tokens on the kinds that carry them, and the generations of events and descriptions`() =
        ok("backendreplacement", "ADDED(a) replaces nothing, generation 0, init source None, no encryption; REMOVED(a).replacedBy = b; ADDED(b).replaces = a with the new codec at generation 0; UPDATED(other) at generation 1 with a generation-1 snapshot; then Empty")

    @Test
    fun `the describe row carries the generation, the init source and its property type, and the four encryption fields, and nothing outside the copy's stamp`() =
        ok("receiverdescribemsf02", "track-property 0x0D -> INIT_SOURCE 2, type 13; inline initData -> INLINE; the four encryption fields verbatim with their bits; the shipped decoder yields InitSource and TrackEncryption and never renders the key; a copy restamped at the v0 size publishes HAS 0, zero slots and empty fields")

    @Test
    fun `the backend maps empty, event, closed and refused answers without folding`() =
        ok("backendreceiverevents", "Empty for DONE; Event with the same token for ADDED; a stale receiver token -> Bridge(REG_STALE); never a MoqResultCode")
}
