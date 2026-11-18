package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The control rows through the real one-DSO bridge, against the real
 * `moq_media_receiver_subscribe_track` / `_unsubscribe_track` /
 * `_track_state` on a session-less receiver: intent is recorded and the
 * state reads PENDING; unsubscribe purges the real object ring; an ended
 * track and a namespace-override track refuse with the C codes; carriers
 * and ownership refuse before anything native. Scenarios in
 * [EndpointChildMain].
 */
internal class ReceiverControlRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `subscribe records intent -- state DISCOVERED then PENDING -- and a repeat is OK without change`() =
        ok("receivercontrolsubscribe", "state -> DISCOVERED; subscribe(NEXT_GROUP, priority 7) -> OK; state -> PENDING; subscribe again (CURRENT, no priority) -> OK, state still PENDING")

    @Test
    fun `unsubscribe purges the track's queued objects from the real ring and leaves the other track's`() =
        ok("receivercontrolpurge", "two desired tracks, three objects queued (v, a, v); unsubscribe(v) -> OK; poll object -> the audio object only, then DONE; the handle still describes")

    @Test
    fun `an ended track refuses subscribe with WRONG_STATE and reads ENDED, and its unsubscribe is still the C OK`() =
        ok("receivercontrolended", "feed an END_OF_TRACK object for a; drain -> TRACK_ENDED; subscribe(a) -> WRONG_STATE; state(a) -> ENDED")

    @Test
    fun `a track dropped by a later catalog generation is REMOVED -- subscribe, unsubscribe and state all answer WRONG_STATE`() =
        ok("receivercontrolremoved", "ingest generation 2 (group 1) without track b; drain -> TRACK_REMOVED(b); subscribe(b) / unsubscribe(b) / state(b) -> WRONG_STATE each; a stays DISCOVERED and subscribable")

    @Test
    fun `a track that declares its own namespace cannot be subscribed -- UNSUPPORTED, exactly`() =
        ok("receivercontrolnamespace", "a catalog track with its own namespace; subscribe -> MOQ_ERR_UNSUPPORTED; state -> DISCOVERED")

    @Test
    fun `malformed carriers and foreign or stale tokens refuse before anything native`() =
        ok("receivercontrolmalformed", "bad start ordinal -> INVAL; priority outside 0..255 -> INVAL; null/short state block -> INVAL untouched; a track under another receiver -> REG_OWNER; after receiver close -> REG_STALE for all three rows")
}
