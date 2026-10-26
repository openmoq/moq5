package org.openmoq.moq5.samples.player

import kotlinx.coroutines.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackRenderCleanupTest {
    @Test fun `successful cleanup releases sync before codecs and surface after video`() {
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender(
            { calls.add("sync"); PlaybackSyncClosed(true, true, null) },
            { calls.add("video") }, { calls.add("audio") }, { calls.add("track") }, { calls.add("surface") },
        )
        assertEquals(listOf("sync", "video", "audio", "track", "surface"), calls)
        assertTrue(result.retained.isEmpty())
        assertNull(result.failure)
    }

    @Test fun `successful flush without release retains every dependent even though PCM quiesced`() {
        val cause = AssertionError("release failed")
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender(
            { calls.add("sync"); PlaybackSyncClosed(true, false, cause) },
            { calls.add("video") }, { calls.add("audio") }, { calls.add("track") }, { calls.add("surface") },
        )
        assertEquals(listOf("sync"), calls)
        assertEquals(PlaybackRetainedResource.entries.toSet(), result.retained)
        assertSame(cause, result.failure)
    }

    @Test fun `throwing sync cleanup retains actual acquired dependents only`() {
        val cause = CancellationException("sync close")
        val result = disposePlaybackRender({ throw cause }, {}, null, null, {})
        assertEquals(setOf(PlaybackRetainedResource.Sync, PlaybackRetainedResource.VideoCodec, PlaybackRetainedResource.InputSurface), result.retained)
        assertSame(cause, result.failure)
    }

    @Test fun `failed video prevents surface disposal but not other safe cleanup and causes stay ordered`() {
        val first = IllegalStateException("flush")
        val second = AssertionError("video")
        val third = CancellationException("audio")
        val fourth = IllegalArgumentException("track")
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender(
            { PlaybackSyncClosed(true, true, first) },
            { calls.add("video"); throw second }, { calls.add("audio"); throw third },
            { calls.add("track"); throw fourth }, { calls.add("surface") },
        )
        assertSame(first, result.failure)
        assertEquals(listOf<Throwable>(second, third, fourth), first.suppressed.toList())
        assertEquals(listOf("video", "audio", "track"), calls)
        assertEquals(setOf(PlaybackRetainedResource.VideoCodec, PlaybackRetainedResource.AudioCodec, PlaybackRetainedResource.AudioTrack, PlaybackRetainedResource.InputSurface), result.retained)
    }

    @Test fun `same cleanup cause is never self suppressed and surface failure remains retained`() {
        val cause = AssertionError("same")
        val result = disposePlaybackRender({ PlaybackSyncClosed(true, true, cause) }, {}, { throw cause }, null, { throw cause })
        assertSame(cause, result.failure)
        assertTrue(cause.suppressed.isEmpty())
        assertEquals(setOf(PlaybackRetainedResource.AudioCodec, PlaybackRetainedResource.InputSurface), result.retained)
    }

    @Test fun `partial acquisition cleans precisely the acquired resources`() {
        assertTrue(disposePlaybackRender(null, null, null, null, null).retained.isEmpty())
        val calls = mutableListOf<String>()
        val result = disposePlaybackRender({ PlaybackSyncClosed(true, true, null) }, { calls.add("v") }, null, null, { calls.add("s") })
        assertEquals(listOf("v", "s"), calls)
        assertTrue(result.retained.isEmpty())
    }

    @Test fun `the direct path has no sync or input surface and a video codec left unclosed retains the renderer`() {
        val cause = IllegalStateException("codec release")
        val retained = disposePlaybackRender(null, { throw cause }, null, null, null)
        assertEquals(setOf(PlaybackRetainedResource.VideoCodec), retained.retained, "the display lease stays owned while the codec may still render")
        assertSame(cause, retained.failure)
        val calls = mutableListOf<String>()
        val released = disposePlaybackRender(null, { calls.add("video") }, null, null, null)
        assertEquals(listOf("video"), calls)
        assertTrue(released.retained.isEmpty(), "only a closed codec lets the host return the display lease")
        assertTrue(disposePlaybackRender(null, null, null, null, null).retained.isEmpty(), "no codec was created, so nothing can render")
    }
}
