package org.openmoq.moq5.samples.player

/**
 * What the playback loop does with each received object, on the accepted media
 * contract rather than beside it.
 *
 * The session used to count bytes and keyframes straight off the object. That
 * measured the transport and nothing else: it never asked whether the bytes
 * were a decodable access unit, whether a status object carried media, or where
 * the unit belonged on the timeline. Every one of those questions already has
 * an accepted answer in `MediaContract.kt`; this is the seam that asks them.
 *
 * It stays free of Android: no decoder, no surface, no renderer. What it
 * produces is the inventory the sample REPORTS — the units it would queue, in
 * order, with their times — so the step that hands them to a real decoder can
 * be added without moving this decision.
 */
internal class PlaybackDecode(private val format: FormatOutcome) {
    /** Access units this decode would queue, in arrival order. */
    var units: Int = 0
        private set

    /** Their total byte count, after framing. */
    var unitBytes: Long = 0
        private set

    /** Sync units among them. */
    var syncUnits: Int = 0
        private set

    /** Objects that carried no media at all. */
    var statusObjects: Int = 0
        private set

    /** Objects this contract refused, with the first reason kept. */
    var rejected: Int = 0
        private set

    var firstRejection: String? = null
        private set

    /** The presentation time of the last unit that would be queued. */
    var lastPresentationUs: Long? = null
        private set

    fun accept(obj: ObjectFacts) {
        for (outcome in AccessUnits.split(format, obj)) {
            when (outcome) {
                is AccessUnitOutcome.Unit -> {
                    units++
                    unitBytes += outcome.bytes.size.toLong()
                    if (outcome.sync) syncUnits++
                    lastPresentationUs = outcome.presentationTimeUs
                }
                AccessUnitOutcome.StatusOnly -> statusObjects++
                is AccessUnitOutcome.Rejected -> {
                    rejected++
                    if (firstRejection == null) firstRejection = outcome.reason
                }
            }
        }
    }

    /** One line naming what this decode did, for the session's report. */
    fun summary(): String {
        val head =
            units.toString() + " access unit(s), " + unitBytes + " framed byte(s), " + syncUnits +
                " sync, " + statusObjects + " status object(s)"
        val reason = firstRejection
        return if (reason == null) head else head + "; " + rejected + " refused, first: " + reason
    }
}
