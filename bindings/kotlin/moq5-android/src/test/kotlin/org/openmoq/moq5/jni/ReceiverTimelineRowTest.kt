package org.openmoq.moq5.jni

import kotlin.test.Test

/**
 * The record rows through the real one-DSO bridge: a session-less receiver
 * with a catalog declaring a SAP event timeline and a media timeline, objects
 * fed through the service's own intake (the host seam routes a timeline
 * track's object to its timeline intake exactly as the pump hook does), and
 * the REAL `poll_sap` / `poll_media_timeline` answering with the record and
 * the family's deferral counter in one block. Scenarios in [EndpointChildMain].
 */
internal class ReceiverTimelineRowTest {
    private fun ok(scenario: String, what: String) {
        val env = TrustedChild.probeArmed + (EndpointAbi.MODE to EndpointAbi.MODE_DRAIN_PENDING)
        TrustedChild.assertOk(EndpointAbi.child(scenario, EndpointAbi.SCRIPTED_TOKEN, env), what)
    }

    @Test
    fun `sap records cross with their track token, the closed type, ept in milliseconds, and the counter at zero`() =
        ok("receivertimelinesap", "poll -> DONE, DEFERRALS 0; feed one SAP object with two records; poll -> (7,0,type 2,ept 0) then (7,1,type 3,ept 2100) with the sap track's token, DEFERRALS 0; poll -> DONE")

    @Test
    fun `media-timeline records cross with millisecond media time, full ids, and wallclock zero as zero`() =
        ok("receivertimelinemt", "feed one media-timeline object with two records; poll -> (mediaTime 0, (7,0), wallclock 0) then (40, (7,1), 1700000000001); DEFERRALS 0; DONE")

    @Test
    fun `an overflowing sap object defers its tail, the block carries the exact counter, and the next independent object delivers the tail`() =
        ok("receivertimelinesapdefer", "feed 300 records; poll 256 in order with DEFERRALS 44 on every answer; DONE with DEFERRALS 44; feed generation 2 with the 300; poll -> exactly 256..299, DEFERRALS still 44")

    @Test
    fun `an overflowing media-timeline object defers likewise with its own counter`() =
        ok("receivertimelinemtdefer", "300 records -> 256 + DEFERRALS 44 on the timeline block, the sap counter untouched; the re-send delivers 256..299")

    @Test
    fun `malformed carriers refuse before any native poll, and foreign or stale tokens are the registry's refusal`() =
        ok("receivertimelinemalformed", "null/short blocks -> INVAL untouched, nothing consumed; wrong root -> REG_OWNER; after close -> REG_STALE for both rows")
}
