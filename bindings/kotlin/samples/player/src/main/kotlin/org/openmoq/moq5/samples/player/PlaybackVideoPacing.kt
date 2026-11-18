package org.openmoq.moq5.samples.player

/** What to do with the decoded frame the renderer holds. */
internal sealed interface PlaybackVideoStep {
    /** Not yet: keep holding it. */
    data object Hold : PlaybackVideoStep

    /** Release it to the display, stamped [atNs] on the monotonic clock. */
    class Render(val atNs: Long) : PlaybackVideoStep

    /** Release it unrendered: it is too late for the audio it belongs with. */
    data object Drop : PlaybackVideoStep
}

/**
 * When each decoded frame goes to the display Surface.
 *
 * Two time domains stay distinct. Media time is the decoder's relative
 * presentation time in nanoseconds; display time is the monotonic clock
 * [nowNs] (System.nanoTime on the device). A SurfaceTexture consumer only
 * records the release timestamp and schedules nothing, so the pacing is the
 * hold itself: the owner asks [step] for the held frame on each pass.
 *
 * With a [master] that leads (the audio being heard), a frame is released
 * once the master reaches its media time. A frame later than [lateDropNs]
 * behind the master is dropped, except the first, so video catches up with
 * audio instead of trailing it. Before the master has started, the first
 * frame is shown at once and later ones wait.
 *
 * Without a master, or while it is detached, video keeps its own time: one
 * anchor pair maps media time onto the display clock, and every frame's
 * deadline is the display anchor plus its media delta. Nothing is dropped on
 * this clock for being late. The anchor is taken at the first frame placed on
 * it, and again whenever the master stops leading, so a handover never
 * presents earlier media than what was shown.
 *
 * On either clock, a frame behind one already shown is dropped, never shown:
 * video does not step back. That happens when objects of two groups arrive
 * interleaved and the receive side did not already drop the older group.
 */
internal class PlaybackVideoPacing(
    private val nowNs: () -> Long,
    private val master: PlaybackMasterClock? = null,
    private val lateDropNs: Long = 50_000_000,
) {
    init { require(lateDropNs > 0) }

    private var anchored = false
    private var anchorClockNs = 0L
    private var anchorMediaNs = 0L

    /** Media time of the last frame released to the display, or null before the first. */
    var presentedNs: Long? = null
        private set

    /** Frames dropped: too late for the master clock, or behind one already shown. */
    var droppedLate: Long = 0
        private set

    /** The monotonic display clock now. */
    fun now(): Long = nowNs()

    /** Decide the held frame at relative media time [mediaNs]. Render and Drop are final for that frame. */
    fun step(mediaNs: Long): PlaybackVideoStep {
        require(mediaNs >= 0) { "relative media time is never negative" }
        // A frame behind one already shown is never shown: video does not step back.
        val shown = presentedNs
        if (shown != null && mediaNs < shown) {
            droppedLate++
            return PlaybackVideoStep.Drop
        }
        val now = nowNs()
        val leader = master
        if (leader != null) {
            when (val reading = leader.reading(now, presentedNs)) {
                PlaybackMasterReading.Pending ->
                    return if (presentedNs == null) rendered(now, mediaNs) else PlaybackVideoStep.Hold
                is PlaybackMasterReading.At -> {
                    anchored = false
                    // Compare without subtracting: both are nonnegative.
                    if (mediaNs > reading.mediaNs) return PlaybackVideoStep.Hold
                    if (presentedNs != null && reading.mediaNs - mediaNs > lateDropNs) {
                        droppedLate++
                        return PlaybackVideoStep.Drop
                    }
                    return rendered(now, mediaNs)
                }
                PlaybackMasterReading.Detached -> Unit
            }
        }
        val deadline = deadline(now, mediaNs)
        if (now < anchorClockNs) throw PlaybackRenderRefused("the monotonic display clock ran backwards")
        return if (now >= deadline) rendered(deadline, mediaNs) else PlaybackVideoStep.Hold
    }

    private fun rendered(atNs: Long, mediaNs: Long): PlaybackVideoStep {
        presentedNs = mediaNs
        return PlaybackVideoStep.Render(atNs)
    }

    /** The display-clock deadline of a frame at [mediaNs]; the first frame on this clock anchors it. */
    private fun deadline(now: Long, mediaNs: Long): Long {
        if (!anchored) {
            anchorClockNs = now
            anchorMediaNs = mediaNs
            anchored = true
        }
        // The anchor frame is shown at once, so a frame before it was dropped above.
        val delta = mediaNs - anchorMediaNs
        if (anchorClockNs > Long.MAX_VALUE - delta) throw PlaybackRenderRefused("video deadline exceeds the monotonic clock range")
        return anchorClockNs + delta
    }
}
