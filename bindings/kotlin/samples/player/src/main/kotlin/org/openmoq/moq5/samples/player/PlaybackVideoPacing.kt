package org.openmoq.moq5.samples.player

/**
 * The Surface the video decoder renders to. With audio, MediaSync owns
 * presentation: it is created once over the display Surface and the decoder
 * renders into its proxy input Surface. Video-only playback acquires no
 * synchronizer: the decoder is configured with the borrowed display Surface.
 */
internal fun <S : Any> playbackVideoTarget(display: S, withAudio: Boolean, createSyncInput: (S) -> S): S =
    if (withAudio) createSyncInput(display) else display

/**
 * Display pacing for video rendered straight to the display Surface.
 *
 * Two time domains stay distinct. Media time is the decoder's relative
 * presentation time in nanoseconds. Display time is the monotonic clock
 * [nowNs] (System.nanoTime on the device). The first rendered frame fixes one
 * anchor pair; every frame's deadline is the display anchor plus its media
 * delta from the media anchor. The owner holds a frame until its deadline and
 * then releases it with that deadline as its timestamp. A SurfaceTexture
 * consumer only records that timestamp; it schedules nothing, so the pacing
 * is the hold itself.
 */
internal class PlaybackVideoPacing(private val nowNs: () -> Long) {
    private var anchored = false
    private var anchorClockNs = 0L
    private var anchorMediaNs = 0L

    /** The display-clock deadline of a frame at relative media time [mediaNs]. */
    fun deadline(mediaNs: Long): Long {
        require(mediaNs >= 0) { "relative media time is never negative" }
        if (!anchored) {
            anchorClockNs = nowNs()
            anchorMediaNs = mediaNs
            anchored = true
        }
        if (mediaNs < anchorMediaNs) throw PlaybackRenderRefused("video output precedes the display pacing anchor")
        val delta = mediaNs - anchorMediaNs
        if (anchorClockNs > Long.MAX_VALUE - delta) throw PlaybackRenderRefused("video deadline exceeds the monotonic clock range")
        return anchorClockNs + delta
    }

    /** True once the display clock has reached [deadlineNs]. Only meaningful after [deadline]. */
    fun due(deadlineNs: Long): Boolean {
        val now = nowNs()
        if (now < anchorClockNs) throw PlaybackRenderRefused("the monotonic display clock ran backwards")
        return now >= deadlineNs
    }
}
