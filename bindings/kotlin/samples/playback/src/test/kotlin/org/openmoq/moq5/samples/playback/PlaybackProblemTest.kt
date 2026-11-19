package org.openmoq.moq5.samples.playback

import kotlin.test.Test
import kotlin.test.assertEquals

class PlaybackProblemTest {
    @Test fun `the engine's own failures are classified by what they mean to a viewer`() {
        assertEquals(PlaybackProblemKind.Unsupported, PlaybackProblem.of(PlaybackRenderRefused(NO_SUPPORTED_SELECTION)).kind)
        assertEquals(PlaybackProblemKind.Playback, PlaybackProblem.of(PlaybackRenderRefused("decoded PCM format changed during playback")).kind)
        assertEquals(PlaybackProblemKind.TrackFailed, PlaybackProblem.of(PlaybackTrackFailedException(PlaybackLane.Audio)).kind)
        val other = PlaybackProblem.of(IllegalArgumentException("odd"))
        assertEquals(PlaybackProblemKind.Other, other.kind)
        assertEquals("odd", other.detail)
    }
}
