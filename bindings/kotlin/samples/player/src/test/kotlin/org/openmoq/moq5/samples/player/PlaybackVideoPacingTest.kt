package org.openmoq.moq5.samples.player

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The video-only display path: which Surface the video decoder renders to, and
 * the pacing that maps relative media time onto the monotonic display clock.
 */
class PlaybackVideoPacingTest {
    private class Clock(var now: Long) { fun read() = now }

    @Test fun `video only renders to the display surface and acquires no synchronizer`() {
        val display = Any()
        var syncs = 0
        val target = playbackVideoTarget(display, withAudio = false) { syncs++; Any() }
        assertSame(display, target, "the decoder is configured with the borrowed display Surface itself")
        assertEquals(0, syncs, "no MediaSync and no proxy input Surface on the video-only path")
    }

    @Test fun `audio with video still routes video through the synchronizer's input surface`() {
        val display = Any()
        val proxy = Any()
        val seen = mutableListOf<Any>()
        val target = playbackVideoTarget(display, withAudio = true) { seen.add(it); proxy }
        assertSame(proxy, target)
        assertEquals(listOf(display), seen, "the synchronizer is created once, over the display Surface")
    }

    @Test fun `the first frame anchors the monotonic clock and later frames keep their media spacing`() {
        val clock = Clock(5_000_000_000)
        val pacing = PlaybackVideoPacing(clock::read)
        assertEquals(5_000_000_000, pacing.deadline(83_332_000))
        clock.now += 1_000
        assertEquals(5_041_666_000, pacing.deadline(124_998_000), "anchor plus the media delta, not the relative media time")
        assertEquals(5_000_000_000, pacing.deadline(83_332_000), "a repeated timestamp maps to the same deadline")
    }

    @Test fun `due is judged on the monotonic clock only`() {
        val clock = Clock(-10)
        val pacing = PlaybackVideoPacing(clock::read)
        val first = pacing.deadline(0)
        val second = pacing.deadline(41_666_000)
        assertTrue(pacing.due(first))
        assertFalse(pacing.due(second))
        clock.now = second - 1
        assertFalse(pacing.due(second))
        clock.now = second
        assertTrue(pacing.due(second))
    }

    @Test fun `media before the anchor, a deadline past the clock range and a clock running backwards are named refusals`() {
        val clock = Clock(Long.MAX_VALUE - 10)
        val pacing = PlaybackVideoPacing(clock::read)
        assertEquals(Long.MAX_VALUE - 10, pacing.deadline(1_000))
        assertEquals(Long.MAX_VALUE, pacing.deadline(1_010))
        assertFailsWith<PlaybackRenderRefused> { pacing.deadline(1_011) }
        assertFailsWith<PlaybackRenderRefused> { pacing.deadline(999) }
        clock.now -= 1
        assertFailsWith<PlaybackRenderRefused> { pacing.due(Long.MAX_VALUE) }
        assertFailsWith<IllegalArgumentException> { PlaybackVideoPacing(clock::read).deadline(-1) }
    }
}
