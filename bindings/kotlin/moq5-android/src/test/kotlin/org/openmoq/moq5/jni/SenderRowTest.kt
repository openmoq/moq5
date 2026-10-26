package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The sender rows through the real one-DSO bridge against the real
 * `moq_media_sender_t` on a live, unconnected root (pre-ready: the C accepts
 * and queues media, refuses the post-readiness operations with WRONG_STATE,
 * and reports no demand). Scenarios in [EndpointChildMain].
 */
internal class SenderRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `attach publishes an owned child, gates the C stop, coexists with a receiver, and close frees the slot`() =
        ok("senderattach", "attach -> OWNED child token (kind 2, full width); stop refused WRONG_STATE while attached; a second attach WRONG_STATE, nothing published; a receiver attaches beside it; close each -> stop OK")

    @Test
    fun `the track rows drive the real sender -- add, pre-ready writes, stats, end, remove, and the pre-ready refusals`() =
        ok("sendertracks", "add -> BORROWED track token (kind 3); probe ready 0 closed 0 fatal 0 demand 0; two writes accepted -> written 2, queued 2, bytes 7; end_track OK, again OK; write after end WRONG_STATE; remove -> OK, again WRONG_STATE; complete and convert_to_vod pre-ready -> WRONG_STATE; track demand 0")

    @Test
    fun `WouldBlock is the C's -- a pre-ready bound of one object, then a delta with no anchor -- and transfers nothing`() =
        ok("senderwouldblock", "pre_ready_max_objects 1: first keyframe accepted, second write WOULD_BLOCK with written still 1; a delta on a fresh track with no anchor WOULD_BLOCK; a RAW object carrying a property block INVAL; last_error latched")

    @Test
    fun `malformed carriers refuse before any native call, and foreign or stale tokens are the registry's refusal`() =
        ok("sendermalformed", "null/short blocks INVAL untouched on attach, add, write, probe, demand, stats; a bad backpressure ordinal INVAL; wrong root REG_OWNER; a track of another sender REG_OWNER; after close REG_STALE")
}
