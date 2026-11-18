package org.openmoq.moq5.samples.player

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertSame

/**
 * When a held frame goes to the display: on the monotonic display clock alone,
 * or following a master clock (the audio being heard) while it leads.
 */
class PlaybackVideoPacingTest {
    private class Clock(var now: Long) { fun read() = now }

    /** A master whose next reading the test sets; it records what video had shown at each reading. */
    private class Master : PlaybackMasterClock {
        var next: PlaybackMasterReading = PlaybackMasterReading.Pending
        val shown = mutableListOf<Long?>()
        override fun reading(nowNs: Long, presentedNs: Long?): PlaybackMasterReading {
            shown += presentedNs
            return next
        }
    }

    private fun at(ms: Long) = PlaybackMasterReading.At(ms * 1_000_000)

    private fun renderedAt(step: PlaybackVideoStep): Long = assertIs<PlaybackVideoStep.Render>(step).atNs

    // -- the display clock -------------------------------------------------------------------

    @Test fun `the first frame anchors the display clock and later frames keep their media spacing`() {
        val clock = Clock(5_000_000_000)
        val pacing = PlaybackVideoPacing(clock::read)
        assertEquals(5_000_000_000, renderedAt(pacing.step(83_332_000)))
        clock.now += 1_000
        assertSame(PlaybackVideoStep.Hold, pacing.step(124_998_000))
        clock.now = 5_041_666_000 - 1
        assertSame(PlaybackVideoStep.Hold, pacing.step(124_998_000))
        clock.now = 5_041_666_000
        assertEquals(5_041_666_000, renderedAt(pacing.step(124_998_000)), "stamped with the anchor plus the media delta")
        clock.now += 500_000_000
        assertEquals(5_083_332_000, renderedAt(pacing.step(166_664_000)), "a late frame shows at once with its own deadline; none is dropped")
        assertEquals(0L, pacing.droppedLate)
    }

    @Test fun `a deadline past the clock range and a clock running backwards are named refusals`() {
        val clock = Clock(Long.MAX_VALUE - 10)
        val pacing = PlaybackVideoPacing(clock::read)
        assertEquals(Long.MAX_VALUE - 10, renderedAt(pacing.step(1_000)))
        assertSame(PlaybackVideoStep.Hold, pacing.step(1_010))
        assertFailsWith<PlaybackRenderRefused> { pacing.step(1_011) }
        clock.now -= 1
        assertFailsWith<PlaybackRenderRefused> { pacing.step(1_010) }
        assertFailsWith<IllegalArgumentException> { PlaybackVideoPacing(clock::read).step(-1) }
    }

    // -- following a master ------------------------------------------------------------------

    @Test fun `before the master starts the first frame shows at once and later frames wait`() {
        val clock = Clock(9_000)
        val master = Master()
        val pacing = PlaybackVideoPacing(clock::read, master)
        assertEquals(9_000, renderedAt(pacing.step(0)))
        assertSame(PlaybackVideoStep.Hold, pacing.step(41_666_000))
        assertSame(PlaybackVideoStep.Hold, pacing.step(41_666_000))
        assertEquals(listOf(null, 0L, 0L), master.shown)
    }

    @Test fun `a leading master releases a frame once it reaches it, stamped now`() {
        val clock = Clock(9_000)
        val master = Master()
        val pacing = PlaybackVideoPacing(clock::read, master)
        master.next = at(0)
        assertEquals(9_000, renderedAt(pacing.step(0)))
        master.next = PlaybackMasterReading.At(41_665_999)
        assertSame(PlaybackVideoStep.Hold, pacing.step(41_666_000))
        clock.now = 50_000
        master.next = PlaybackMasterReading.At(41_666_000)
        assertEquals(50_000, renderedAt(pacing.step(41_666_000)))
        assertEquals(41_666_000, pacing.presentedNs)
    }

    @Test fun `a frame more than the bound behind the master is dropped, except the first`() {
        val master = Master()
        val pacing = PlaybackVideoPacing({ 1 }, master)
        master.next = at(1_000)
        assertIs<PlaybackVideoStep.Render>(pacing.step(0), "the first frame is never dropped")
        assertSame(PlaybackVideoStep.Drop, pacing.step(949_999_999))
        assertEquals(0L, pacing.presentedNs, "a dropped frame was not shown")
        assertIs<PlaybackVideoStep.Render>(pacing.step(950_000_000), "exactly the bound behind is still shown")
        assertEquals(1L, pacing.droppedLate)
    }

    @Test fun `a detached master hands over at the held frame, and video never steps back across handovers`() {
        val clock = Clock(1_000_000_000)
        val master = Master()
        val pacing = PlaybackVideoPacing(clock::read, master)
        val shown = mutableListOf<Long>()
        fun place(mediaMs: Long): PlaybackVideoStep =
            pacing.step(mediaMs * 1_000_000).also { if (it is PlaybackVideoStep.Render) shown += mediaMs }

        master.next = at(100)
        assertIs<PlaybackVideoStep.Render>(place(100))
        assertSame(PlaybackVideoStep.Hold, place(140))
        // The audio stops leading: the held frame anchors the display clock now.
        master.next = PlaybackMasterReading.Detached
        clock.now = 3_000_000_000
        assertEquals(3_000_000_000, renderedAt(place(140)))
        assertSame(PlaybackVideoStep.Hold, place(180))
        clock.now = 3_040_000_000
        assertEquals(3_040_000_000, renderedAt(place(180)))
        // Audio leads again but behind what was shown: the next frame waits instead of jumping.
        master.next = at(150)
        assertSame(PlaybackVideoStep.Hold, place(220))
        master.next = at(220)
        clock.now = 3_100_000_000
        assertEquals(3_100_000_000, renderedAt(place(220)))
        // Detached again: a fresh anchor at the frame now held, not the old anchor's schedule.
        master.next = PlaybackMasterReading.Detached
        clock.now = 9_000_000_000
        assertEquals(9_000_000_000, renderedAt(place(260)))
        assertEquals(listOf(100L, 140L, 180L, 220L, 260L), shown)
        assertEquals(shown.sorted(), shown, "presented media time never decreases")
        assertEquals(listOf<Long?>(null, 100_000_000, 100_000_000, 140_000_000, 140_000_000, 180_000_000, 180_000_000, 220_000_000), master.shown)
    }

    @Test fun `a frame behind one already shown is dropped on either clock, never shown`() {
        val clock = Clock(1_000)
        val display = PlaybackVideoPacing(clock::read)
        assertIs<PlaybackVideoStep.Render>(display.step(100_000_000))
        assertSame(PlaybackVideoStep.Drop, display.step(60_000_000), "an older group's frame after the anchor")
        assertEquals(100_000_000, display.presentedNs)
        val master = Master()
        val led = PlaybackVideoPacing(clock::read, master)
        master.next = at(100)
        assertIs<PlaybackVideoStep.Render>(led.step(100_000_000))
        master.next = at(110)
        assertSame(PlaybackVideoStep.Drop, led.step(99_000_000), "behind what was shown, though within the late bound")
        assertEquals(listOf(1L, 1L), listOf(display.droppedLate, led.droppedLate))
    }

    @Test fun `without a master nothing is presented before the first step`() {
        assertNull(PlaybackVideoPacing({ 0 }).presentedNs)
        assertFailsWith<IllegalArgumentException> { PlaybackVideoPacing({ 0 }, null, 0) }
    }
}
