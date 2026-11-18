package org.openmoq.moq5.samples.player

internal enum class PlaybackRetainedResource { VideoCodec, AudioCodec, AudioTrack }

/** Caller must retain the enclosing renderer while [retained] is nonempty. */
internal class PlaybackRenderClosed(val retained: Set<PlaybackRetainedResource>, val failure: Throwable?)

/**
 * The actual renderer's disposal, also executable on host fixtures. Null
 * means this resource was never acquired, not disposal success. Nothing is
 * borrowed across these resources: written PCM is copied into the track, and
 * the video decoder renders to the caller's Surface, which the caller frees
 * only once no VideoCodec is retained. Every disposal is attempted; a failure
 * retains only that resource and never skips another's cleanup. All
 * callbacks are synchronous, nonreentrant and attempted at most once by the
 * enclosing owner's cached close. No OOM recovery is promised.
 */
internal fun disposePlaybackRender(
    video: (() -> Unit)?,
    audio: (() -> Unit)?,
    track: (() -> Unit)?,
): PlaybackRenderClosed {
    val retained = linkedSetOf<PlaybackRetainedResource>()
    var failure: Throwable? = null
    fun dispose(kind: PlaybackRetainedResource, action: (() -> Unit)?) {
        if (action == null) return
        val raised = runCatching(action).exceptionOrNull() ?: return
        retained.add(kind)
        val first = failure
        if (first == null) failure = raised
        else if (raised !== first && first.suppressed.none { it === raised }) first.addSuppressed(raised)
    }
    dispose(PlaybackRetainedResource.VideoCodec, video)
    dispose(PlaybackRetainedResource.AudioCodec, audio)
    dispose(PlaybackRetainedResource.AudioTrack, track)
    return PlaybackRenderClosed(retained.toSet(), failure)
}
