package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackRenderCleanupTest {
    @Test fun `successful cleanup disposes every acquired resource once, video first`() {
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender({ calls.add("video") }, { calls.add("audio") }, { calls.add("track") })
        assertEquals(listOf("video", "audio", "track"), calls)
        assertTrue(result.retained.isEmpty())
        assertNull(result.failure)
    }

    @Test fun `each failure retains only its own resource, skips no other cleanup, and causes stay ordered`() {
        val first = AssertionError("video")
        val second = CancellationException("audio")
        val third = IllegalArgumentException("track")
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender(
            { calls.add("video"); throw first }, { calls.add("audio"); throw second }, { calls.add("track"); throw third },
        )
        assertEquals(listOf("video", "audio", "track"), calls)
        assertSame(first, result.failure)
        assertEquals(listOf<Throwable>(second, third), first.suppressed.toList())
        assertEquals(PlaybackRetainedResource.entries.toSet(), result.retained)
    }

    @Test fun `a video codec left unclosed retains the renderer, so the display lease stays owned`() {
        val cause = IllegalStateException("codec release")
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender({ throw cause }, { calls.add("audio") }, { calls.add("track") })
        assertEquals(setOf(PlaybackRetainedResource.VideoCodec), result.retained)
        assertEquals(listOf("audio", "track"), calls, "audio cleanup does not depend on the video decoder")
        assertSame(cause, result.failure)
    }

    @Test fun `the same cause is never self suppressed`() {
        val cause = AssertionError("same")
        val result = disposePlaybackRender({}, { throw cause }, { throw cause })
        assertSame(cause, result.failure)
        assertTrue(cause.suppressed.isEmpty())
        assertEquals(setOf(PlaybackRetainedResource.AudioCodec, PlaybackRetainedResource.AudioTrack), result.retained)
    }

    @Test fun `partial acquisition cleans precisely the acquired resources`() {
        assertTrue(disposePlaybackRender(null, null, null).retained.isEmpty(), "no codec was created, so nothing can render")
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender({ calls.add("video") }, null, null)
        assertEquals(listOf("video"), calls)
        assertTrue(result.retained.isEmpty(), "only a closed codec lets the host return the display lease")
    }
}
