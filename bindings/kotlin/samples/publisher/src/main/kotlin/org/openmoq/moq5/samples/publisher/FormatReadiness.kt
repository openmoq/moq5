package org.openmoq.moq5.samples.publisher

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration

/**
 * The two configurations a live preparation waits for, each resolved by the
 * validator in its own time. Either may resolve first, and each is observed
 * as it resolves; what waits for BOTH is track admission. Each slot must
 * resolve a format of ITS kind -- a valid audio format in the video slot is
 * a contract failure, refused before either add. Storage is one deferred per
 * track, fixed. The producers are borrowed: the wait never cancels them.
 */
internal class FormatSources(val video: Deferred<CompleteTrackFormat>, val audio: Deferred<CompleteTrackFormat>)

/**
 * The live publication over configurations that are still being resolved.
 *
 * ONE finite, positive [budget] bounds resolving the PAIR, measured from
 * when the preparation begins; it is checked here, before any capture is
 * started or track added, and never restarts when the first configuration
 * arrives. Both complete configurations are held before the first add, so
 * arrival order does not change video-then-audio creation and no lane runs
 * early. The deadline scopes configuration acquisition only: a commit-grade
 * add or a cleanup that outlives it is the accepted ownership contract's.
 * An expired budget is a [Preparation.Refused] naming the track(s) still
 * missing; a cancellation or a source's own failure keeps its identity.
 * The coordinator's terminal arbitration, cancellation, joining and owner
 * disposition are unchanged: the wait is the same supervised preparation
 * child, and the adds and lanes are [livePublication]'s.
 *
 * [keying] is the audio keying the capture's PCM reads go through, or null
 * when nothing keys them. Once the audio format has arrived its sample rate
 * and access-unit length must agree with that keying's, checked BEFORE
 * either add: a disagreement is a [Preparation.Refused] naming it, and the
 * keying is neither replaced nor read.
 */
internal fun <H : Any> livePublication(
    owner: TrackOwner<H>,
    tracks: TrackOperations<H>,
    events: BroadcastEvents,
    admission: PublicationAdmission,
    formats: FormatSources,
    budget: Duration,
    limits: AvcPayloadLimits,
    clock: PlatformCaptureClock,
    origin: Long,
    keying: AudioKeying?,
): Publication.Live {
    require(budget.isFinite() && budget.isPositive()) { "the format budget is a positive, finite duration, not $budget" }
    return Publication.Live(owner) {
        // Two fixed slots. Each source leaves the wait set as it resolves, so
        // neither waits behind the other; at most two acquisitions, no
        // polling. Only THIS timeout's own expiry is an absence: a source's
        // failure, a nested timeout or a cancellation leaves as itself. The
        // sources are borrowed -- leaving the wait cancels neither producer.
        var video: CompleteTrackFormat? = null
        var audio: CompleteTrackFormat? = null
        val acquired =
            withTimeoutOrNull(budget) {
                while (video == null || audio == null) {
                    select<Unit> {
                        if (video == null) formats.video.onAwait { video = it }
                        if (audio == null) formats.audio.onAwait { audio = it }
                    }
                }
            }
        if (acquired == null) {
            val missing = listOfNotNull("video".takeIf { video == null }, "audio".takeIf { audio == null })
            val noun = if (missing.size == 1) "format" else "formats"
            Preparation.Refused("the ${missing.joinToString(" and ")} $noun did not arrive within $budget")
        } else {
            val first = checkNotNull(video)
            val second = checkNotNull(audio)
            require(first.kind == MediaKind.Video) { "the video source resolved an audio format" }
            require(second.kind == MediaKind.Audio) { "the audio source resolved a video format" }
            val disagreement = keying?.let { audioKeyingDisagreement(it, (second.authority as FormatAuthority.Aac).facts) }
            if (disagreement != null) return@Live Preparation.Refused(disagreement)
            // The budget is over: the commit-grade adds and the lanes are the
            // accepted preparation's, under the coordinator's own settlement.
            // The validated formats themselves: each lane compares and
            // converts against the authority its track was validated with.
            livePublication(owner, tracks, events, admission, first, second, limits, clock, origin).prepare()
        }
    }
}
