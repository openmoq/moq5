package org.openmoq.moq5.samples.playback

import org.openmoq.moq5.MoqVersion
import org.openmoq.moq5.ReceiverStats

/** What a running session tells its UI as typed facts, beside its report lines. */
public sealed interface PlaybackEvent {
    /** The endpoint is established at [version]; null when the transport reported none. */
    public data class Connected(public val version: MoqVersion?) : PlaybackEvent

    /** The tracks chosen for playback, as their decoders are configured. */
    public data class Selected(public val video: VideoTrackInfo, public val audio: AudioTrackInfo?) : PlaybackEvent

    /** One receiver statistics sample, taken while playing when the request asked for them. */
    public data class Stats(public val stats: ReceiverStats) : PlaybackEvent
}

/** A selected video track: its decoder MIME type and coded size. */
public data class VideoTrackInfo(public val mime: String, public val width: Int, public val height: Int)

/** A selected audio track: its decoder MIME type, sample rate and channel count. */
public data class AudioTrackInfo(public val mime: String, public val sampleRate: Int, public val channels: Int)
