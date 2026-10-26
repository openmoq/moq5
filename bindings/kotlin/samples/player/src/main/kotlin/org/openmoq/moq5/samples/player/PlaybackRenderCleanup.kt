package org.openmoq.moq5.samples.player

internal enum class PlaybackRetainedResource { Sync, VideoCodec, AudioCodec, AudioTrack, InputSurface }

/** Caller must retain the enclosing renderer while [retained] is nonempty. */
internal class PlaybackRenderClosed(val retained: Set<PlaybackRetainedResource>, val failure: Throwable?)

/**
 * The actual renderer's dependency-ordered disposal, also executable on host
 * fixtures. Null means this resource was never acquired, not disposal success.
 * Sync release proves attached-resource safety. Codec-close success proves
 * its input Surface can be disposed. A failure retains only the resources
 * whose disposal is unproved, and cannot skip unrelated safe cleanup.
 * All callbacks are synchronous, nonreentrant and attempted at most once by
 * the enclosing owner's cached close. No OOM recovery is promised.
 */
internal fun disposePlaybackRender(
    sync: (() -> PlaybackSyncClosed)?,
    video: (() -> Unit)?,
    audio: (() -> Unit)?,
    track: (() -> Unit)?,
    surface: (() -> Unit)?,
): PlaybackRenderClosed {
    val retained = linkedSetOf<PlaybackRetainedResource>()
    var failure: Throwable? = null
    fun keep(cause: Throwable?) {
        if (cause == null) return
        val first = failure
        if (first == null) failure = cause
        else if (cause !== first && first.suppressed.none { it === cause }) first.addSuppressed(cause)
    }
    if (sync != null) {
        val result = runCatching(sync)
        keep(result.exceptionOrNull())
        val closed = result.getOrNull()
        keep(closed?.failure)
        if (closed?.released != true) {
            retained.add(PlaybackRetainedResource.Sync)
            if (video != null) retained.add(PlaybackRetainedResource.VideoCodec)
            if (audio != null) retained.add(PlaybackRetainedResource.AudioCodec)
            if (track != null) retained.add(PlaybackRetainedResource.AudioTrack)
            if (surface != null) retained.add(PlaybackRetainedResource.InputSurface)
            return PlaybackRenderClosed(retained.toSet(), failure)
        }
    }
    fun dispose(kind: PlaybackRetainedResource, action: (() -> Unit)?): Boolean {
        if (action == null) return true
        val raised = runCatching(action).exceptionOrNull()
        keep(raised)
        if (raised != null) retained.add(kind)
        return raised == null
    }
    val videoClosed = dispose(PlaybackRetainedResource.VideoCodec, video)
    dispose(PlaybackRetainedResource.AudioCodec, audio)
    dispose(PlaybackRetainedResource.AudioTrack, track)
    if (videoClosed) dispose(PlaybackRetainedResource.InputSurface, surface)
    else if (surface != null) retained.add(PlaybackRetainedResource.InputSurface)
    return PlaybackRenderClosed(retained.toSet(), failure)
}
