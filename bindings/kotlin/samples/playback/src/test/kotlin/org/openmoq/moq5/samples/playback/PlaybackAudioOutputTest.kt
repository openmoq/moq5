package org.openmoq.moq5.samples.playback

import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The audio clock video follows: what it reads from the track, how written
 * PCM maps to media time, and when audio stops and starts leading.
 */
class PlaybackAudioOutputTest {
    private class Clock(var now: Long = 5_000_000_000) { fun read() = now }

    /** Takes [room] bytes (a negative [error] instead), has consumed [head] frames, and reports [stamp]. */
    private class Sink : PlaybackAudioSink {
        var room = Int.MAX_VALUE
        var error = 0
        var head = 0
        var stamp: PlaybackAudioTimestamp? = null
        var plays = 0
        override fun write(bytes: ByteBuffer): Int {
            if (error < 0) return error
            val n = minOf(bytes.remaining(), room)
            bytes.position(bytes.position() + n)
            if (room != Int.MAX_VALUE) room -= n
            return n
        }
        override fun head(): Int = head
        override fun timestamp(): PlaybackAudioTimestamp? = stamp
        override fun play() { plays++ }
    }

    /** Stereo PCM16 at 48 kHz: 48 frames per millisecond, 4 bytes per frame. */
    private class Rig {
        val clock = Clock()
        val sink = Sink()
        val notices = mutableListOf<String>()
        val formats = mutableListOf<PlaybackOutputFormat>()
        val out = PlaybackAudioOutput({ formats += it; PlaybackAudioOpened(sink, 48_000, 4) }, clock::read, { notices += it })
        private var index = 0
        fun configure() = out.configure(PlaybackOutputFormat("audio/raw", sampleRate = 48_000, channels = 2))
        fun lease(ptsUs: Long, frames: Int, end: Boolean = false) =
            PlaybackDecodedOutput(index++, ByteBuffer.allocate(frames * 4), frames * 4, ptsUs, end, false)
        fun read(presentedNs: Long? = null) = out.reading(clock.now, presentedNs)
        fun readMs(presentedNs: Long? = null): Long = assertIs<PlaybackMasterReading.At>(read(presentedNs)).mediaNs / 1_000_000
        fun readNs(presentedNs: Long? = null): Long = assertIs<PlaybackMasterReading.At>(read(presentedNs)).mediaNs
        /** Leading at 20 ms of a 1 s output from 0, then a head that stops: detached after the stall bound. */
        fun detachByStall() {
            configure()
            assertTrue(out.write(lease(0, 48_000)))
            sink.head = 960
            assertEquals(20, readMs())
            assertEquals(20, readMs())
            clock.now += 2_000_000_000
            assertSame(PlaybackMasterReading.Detached, read())
            assertEquals(listOf("audio output stalled; playing video only"), notices)
        }
    }

    @Test fun `configure opens the sink once from the decoder format and starts it playing`() {
        val r = Rig()
        r.configure()
        assertEquals(1, r.sink.plays)
        assertEquals(listOf(PlaybackOutputFormat("audio/raw", sampleRate = 48_000, channels = 2)), r.formats)
        assertFailsWith<IllegalStateException> { r.configure() }
        assertEquals(1, r.sink.plays)
    }

    @Test fun `PCM before configuration or after input end is refused`() {
        val r = Rig()
        assertFailsWith<IllegalStateException> { r.out.write(r.lease(0, 1)) }
        r.configure()
        r.out.endInput()
        assertFailsWith<IllegalStateException> { r.out.write(r.lease(0, 1)) }
    }

    @Test fun `nothing heard is pending, then the head maps written frames to their media time`() {
        val r = Rig()
        r.configure()
        assertTrue(r.out.write(r.lease(1_000_000, 4_800)))
        assertEquals(4_800L, r.out.writtenFrames)
        assertSame(PlaybackMasterReading.Pending, r.read())
        r.sink.head = 2_400
        assertEquals(1_050, r.readMs())
        assertTrue(r.out.leading)
    }

    @Test fun `the platform timestamp leads once it exists, extrapolated on the monotonic clock, never past what was written or backwards`() {
        val r = Rig()
        r.configure()
        assertTrue(r.out.write(r.lease(1_000_000, 4_800)))
        val t0 = r.clock.now
        r.sink.head = 960
        assertEquals(1_020, r.readMs(), "no timestamp yet: the head")
        r.clock.now = t0 + 50_000_000
        r.sink.stamp = PlaybackAudioTimestamp(1_440, t0 + 40_000_000)
        assertEquals(1_040, r.readMs(), "1440 frames at t0+40 ms, plus 10 ms")
        r.clock.now = t0 + 100_000_000
        assertEquals(1_090, r.readMs(), "the same reading extrapolated 60 ms")
        r.clock.now = t0 + 150_000_000
        assertEquals(1_100, r.readMs(), "never beyond the 4800 frames written")
        r.clock.now = t0 + 200_000_000
        r.sink.stamp = PlaybackAudioTimestamp(480, t0 + 200_000_000)
        assertEquals(1_100, r.readMs(), "a lower reading never moves the clock backwards")
    }

    @Test fun `a gap between outputs stays a gap on the clock and continuous outputs need no new anchor`() {
        val r = Rig()
        r.configure()
        assertTrue(r.out.write(r.lease(0, 4_800)))
        assertTrue(r.out.write(r.lease(100_000, 4_800)))
        assertTrue(r.out.write(r.lease(1_000_000, 4_800)))
        r.sink.head = 7_200
        assertEquals(150, r.readMs())
        r.sink.head = 9_599
        assertEquals(199_979_166, r.readNs())
        r.sink.head = 9_600
        assertEquals(1_000, r.readMs(), "the third output starts at its own media time")
        r.sink.head = 12_000
        assertEquals(1_050, r.readMs())
    }

    @Test fun `a full sink keeps the output held until all of it is written, and a partial frame is refused`() {
        val r = Rig()
        r.configure()
        val lease = r.lease(0, 4)
        r.sink.room = 8
        assertFalse(r.out.write(lease))
        assertFalse(r.out.write(lease), "still full: still held")
        r.sink.room = 8
        assertTrue(r.out.write(lease))
        assertEquals(4L, r.out.writtenFrames)
        r.sink.room = 6
        assertFailsWith<PlaybackRenderRefused> { r.out.write(r.lease(80, 4)) }
    }

    @Test fun `audio that never starts detaches after the stall bound and says so once`() {
        val r = Rig()
        assertSame(PlaybackMasterReading.Pending, r.read(), "no track yet")
        r.clock.now += 1_999_999_999
        r.configure()
        assertTrue(r.out.write(r.lease(0, 4_800)))
        assertSame(PlaybackMasterReading.Pending, r.read(), "opened and written, but nothing consumed")
        r.clock.now += 1
        assertSame(PlaybackMasterReading.Detached, r.read())
        assertSame(PlaybackMasterReading.Detached, r.read())
        assertEquals(listOf("audio output did not start; playing video only"), r.notices)
        assertFalse(r.out.leading)
        assertEquals(1, r.out.detaches)
    }

    @Test fun `a head that stops detaches, and refused PCM is then dropped instead of held`() {
        val r = Rig()
        r.detachByStall()
        r.sink.room = 0
        val lease = r.lease(1_000_000, 4_800)
        assertTrue(r.out.write(lease), "dropped, so its decoder gets it back")
        assertEquals(19_200L, r.out.droppedBytes)
        assertFalse(lease.bytes!!.hasRemaining())
        assertEquals(1, r.notices.size)
    }

    @Test fun `a full sink that does not consume is a stall even with no video waiting`() {
        val r = Rig()
        r.configure()
        r.sink.room = 0
        val lease = r.lease(0, 4_800)
        assertFalse(r.out.write(lease))
        r.clock.now += 2_000_000_000
        assertTrue(r.out.write(lease), "detached: dropped rather than held")
        assertEquals(listOf("audio output did not start; playing video only"), r.notices)
    }

    @Test fun `a sink error detaches at once and names its code`() {
        val r = Rig()
        r.configure()
        r.sink.error = -6
        assertTrue(r.out.write(r.lease(0, 4_800)))
        assertEquals(listOf("audio output failed (AudioTrack write returned -6); playing video only"), r.notices)
        assertSame(PlaybackMasterReading.Detached, r.read())
    }

    @Test fun `audio leads again after sustained progress, once it has reached what video showed`() {
        val r = Rig()
        r.detachByStall()
        // Video carried on alone and has shown 300 ms. The head moves again, 100 ms per 100 ms.
        val shown = 300_000_000L
        var steps = 0
        var reading: PlaybackMasterReading
        do {
            r.clock.now += 100_000_000
            r.sink.head += 4_800
            steps++
            reading = r.read(shown)
        } while (reading is PlaybackMasterReading.Detached && steps < 20)
        assertEquals(6, steps, "the first move starts the count; 500 ms more is sustained progress")
        assertEquals(620, assertIs<PlaybackMasterReading.At>(reading).mediaNs / 1_000_000)
        assertEquals(listOf("audio output stalled; playing video only", "audio output resumed"), r.notices)
        assertTrue(r.out.leading)
        assertEquals(1, r.out.resumes)
    }

    @Test fun `recovered audio behind what video showed keeps video on its own clock until it catches up`() {
        val r = Rig()
        r.detachByStall()
        val shown = 900_000_000L
        val readings = mutableListOf<PlaybackMasterReading>()
        repeat(12) {
            r.clock.now += 100_000_000
            r.sink.head += 4_800
            readings += r.read(shown)
        }
        val first = readings.indexOfFirst { it is PlaybackMasterReading.At }
        assertEquals(8, first, "recovered after six steps, but leads only once it has reached 900 ms")
        assertEquals(920, (readings[first] as PlaybackMasterReading.At).mediaNs / 1_000_000)
        assertEquals(1, r.notices.count { it == "audio output resumed" })
    }

    @Test fun `a pause longer than the resume gap restarts the count`() {
        val r = Rig()
        r.detachByStall()
        repeat(4) {
            r.clock.now += 100_000_000
            r.sink.head += 4_800
            assertSame(PlaybackMasterReading.Detached, r.read())
        }
        r.clock.now += 300_000_000
        assertSame(PlaybackMasterReading.Detached, r.read(), "no progress for 300 ms")
        repeat(5) {
            r.clock.now += 100_000_000
            r.sink.head += 4_800
            assertSame(PlaybackMasterReading.Detached, r.read(), "counting again from the restart")
        }
        r.clock.now += 100_000_000
        r.sink.head += 4_800
        assertIs<PlaybackMasterReading.At>(r.read())
    }

    @Test fun `a pause that ends with the next move still restarts the count`() {
        val r = Rig()
        r.detachByStall()
        repeat(5) {
            r.clock.now += 100_000_000
            r.sink.head += 4_800
            assertSame(PlaybackMasterReading.Detached, r.read(), "400 ms counted from the first move")
        }
        r.clock.now += 250_000_000
        assertSame(PlaybackMasterReading.Detached, r.read(), "still for exactly the resume gap: not yet too long")
        r.clock.now += 200_000_000
        r.sink.head += 4_800
        assertSame(PlaybackMasterReading.Detached, r.read(), "this move ends a 450 ms pause: the count starts again from it")
        repeat(4) {
            r.clock.now += 100_000_000
            r.sink.head += 4_800
            assertSame(PlaybackMasterReading.Detached, r.read(), "counting from the move that ended the pause")
        }
        r.clock.now += 100_000_000
        r.sink.head += 4_800
        assertIs<PlaybackMasterReading.At>(r.read())
        assertEquals(listOf("audio output stalled; playing video only", "audio output resumed"), r.notices)
    }

    @Test fun `once input ends, audio that has played out or stopped moving stops leading without a report`() {
        val played = Rig()
        played.configure()
        assertTrue(played.out.write(played.lease(0, 4_800, end = true)))
        played.out.endInput()
        played.sink.head = 4_800
        assertSame(PlaybackMasterReading.Detached, played.read())
        assertFalse(played.out.leading)
        assertEquals(emptyList(), played.notices)

        val stuck = Rig()
        stuck.configure()
        assertTrue(stuck.out.write(stuck.lease(0, 4_800)))
        stuck.out.endInput()
        stuck.sink.head = 4_000
        assertEquals(83, stuck.readMs())
        stuck.readMs()
        stuck.clock.now += 2_000_000_000
        assertSame(PlaybackMasterReading.Detached, stuck.read())
        assertEquals(emptyList(), stuck.notices, "the end of the stream is not an output failure")
    }

    @Test fun `the head is a 64-bit count across a 32-bit wrap, and any other backward step is refused`() {
        val wraps = Rig()
        wraps.configure()
        assertTrue(wraps.out.write(wraps.lease(0, 4_800)))
        wraps.sink.head = 0xffff_ff00.toInt()
        assertEquals(100, wraps.readMs(), "capped at what was written")
        wraps.sink.head = 0x100
        assertEquals(100, wraps.readMs(), "the wrap continues the count")

        val back = Rig()
        back.configure()
        assertTrue(back.out.write(back.lease(0, 4_800)))
        back.sink.head = 1_000
        back.readMs()
        back.sink.head = 500
        assertFailsWith<PlaybackRenderRefused> { back.read() }
    }
}
