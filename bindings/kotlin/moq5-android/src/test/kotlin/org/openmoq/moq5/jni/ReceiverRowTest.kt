package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The receiver rows through the real one-DSO bridge, against a LIVE scripted
 * endpoint (the drain-pending host mode leaves the facade non-terminal, so
 * the endpoint accepts attachments). Each row a child JVM. Scenarios in
 * [EndpointChildMain].
 */
internal class ReceiverRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `attach publishes an owned child token, the C stop refuses while it is attached, close frees the slot, then stop and destroy succeed`() =
        ok("receiverattach", "attach -> child token (kind 2, full width); stop -> WRONG_STATE while attached; close -> OK; stop -> OK; destroy -> OK")

    @Test
    fun `a closed receiver is stale to close again, a foreign root is not its owner, and another thread is refused`() =
        ok("receiverclosestale", "close twice -> REG_STALE; close under another root -> REG_OWNER; close from a foreign thread -> REG_THREAD")

    @Test
    fun `malformed direct attach calls fail before the root is pinned and publish nothing`() =
        ok("receivermalformed", "null/short out, empty namespace, empty part, 33 parts, bad overflow, bad time mode, negative bounds, empty catalog, stale root -> INVAL or the registry's code; out[] untouched; the root still stops cleanly")

    @Test
    fun `a receiver attached but refused by the registry is destroyed at once, so the endpoint can still stop`() =
        ok("receiverinsertfault", "with the next child insertion refused: attach -> the registry's CAPACITY code, nothing published, and stop -> OK (the attachment count was restored); a later attach succeeds")

    @Test
    fun `the backend maps the endpoint's refusal to a MoqResultCode and a registry refusal to a Bridge failure`() =
        ok("backendreceiver", "WRONG_STATE -> Refused(MoqResultCode(-5)); a stale child or root -> Bridge(BridgeFailure(..., REG_STALE)), never a code")

    @Test
    fun `a second attach while one is attached is the endpoint's WRONG_STATE, carried exactly`() =
        ok("receiversecond", "attach; attach -> MOQ_ERR_WRONG_STATE (-5) and out[] untouched; close; attach -> OK")
}
