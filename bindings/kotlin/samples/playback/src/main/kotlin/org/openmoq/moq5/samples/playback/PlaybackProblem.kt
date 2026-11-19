package org.openmoq.moq5.samples.playback

import org.openmoq.moq5.EndpointClosedException
import org.openmoq.moq5.EndpointCreateFailedException
import org.openmoq.moq5.EndpointTimeoutException
import org.openmoq.moq5.ReceiverAttachFailedException
import org.openmoq.moq5.ReceiverFatalException
import org.openmoq.moq5.TrackUnavailableException
import org.openmoq.moq5.TrustPolicyUnsupportedException

/** What kind of failure ended a session, so a UI can explain it in its own words. */
public enum class PlaybackProblemKind {
    /** The relay could not be reached, or the connection closed. */
    Unreachable,

    /** The relay did not answer in time. */
    TimedOut,

    /** The relay or the broadcast refused or ended the session. */
    Refused,

    /** A selected track failed or became unavailable. */
    TrackFailed,

    /** The broadcast offers no video and audio this engine decodes together. */
    Unsupported,

    /** Media arrived that playback could not use, or a playback bound was reached. */
    Playback,

    /** Anything else; [PlaybackProblem.detail] says what. */
    Other,
}

/** A session failure as a [kind], with the failure's own [detail] text. */
public class PlaybackProblem(public val kind: PlaybackProblemKind, public val detail: String) {
    public companion object {
        /** Classify a [SessionUpdate.Failed] failure. */
        public fun of(failure: Exception): PlaybackProblem {
            val detail = failure.message ?: failure::class.simpleName ?: "unknown failure"
            val kind =
                when (failure) {
                    is EndpointTimeoutException -> PlaybackProblemKind.TimedOut
                    is EndpointCreateFailedException, is EndpointClosedException -> PlaybackProblemKind.Unreachable
                    is ReceiverAttachFailedException, is ReceiverFatalException,
                    is TrustPolicyUnsupportedException, is PlaybackTerminatedException,
                    -> PlaybackProblemKind.Refused
                    is PlaybackTrackFailedException, is TrackUnavailableException -> PlaybackProblemKind.TrackFailed
                    is PlaybackRenderRefused ->
                        if (failure.reason == NO_SUPPORTED_SELECTION) PlaybackProblemKind.Unsupported else PlaybackProblemKind.Playback
                    else -> PlaybackProblemKind.Other
                }
            return PlaybackProblem(kind, detail)
        }
    }
}
