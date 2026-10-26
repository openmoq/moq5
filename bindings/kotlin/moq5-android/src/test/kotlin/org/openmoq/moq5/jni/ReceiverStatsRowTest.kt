package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The stats row through the real one-DSO bridge: the real
 * `moq_media_receiver_get_stats` after real intake, every field in its slot.
 * Scenarios in [EndpointChildMain].
 */
internal class ReceiverStatsRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `the real stats cross field by field -- received and queued objects, a purge's drops, and the SAP deferrals`() =
        ok("receiverstats", "at rest every slot 0; feed two objects (4 and 6 bytes) -> received 2, queued 2, bytes 10; poll one -> queued 1, bytes 6; unsubscribe purges -> queued 0, bytes 0, dropped exactly 1, keyframes dropped 0 (no LOC frame marking), received still 2; 300 SAP records -> sap deferrals 44, timeline deferrals 0; paused 0, catalog complete 0")

    @Test
    fun `catalog completion latches from an isComplete catalog and is not the receiver's terminal`() =
        ok("receiverstatscomplete", "a generation-2 catalog with isComplete:true and no tracks -> catalog complete 1, catalog drops 0; one generation-3 catalog object after completion -> catalog drops exactly 1, complete still 1; the receiver is not fatal")

    @Test
    fun `malformed carriers refuse before any native read, and foreign or stale tokens are the registry's refusal`() =
        ok("receiverstatsmalformed", "null block -> INVAL; short block -> INVAL untouched; wrong root -> REG_OWNER; after receiver close -> REG_STALE")
}
