package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

@OptIn(ExperimentalCoroutinesApi::class)
class PlaybackRenderPumpTest {
    private class Codec(val name: String, val log: MutableList<String>) : PlaybackCodecBackend {
        val queued = mutableListOf<Pair<Int, Long>>()
        val events = ArrayDeque<PlaybackCodecEvent>()
        var blocked = false
        var releaseFailure: Throwable? = null
        var bytes = ByteBuffer.allocate(128)
        override fun inputIndex(): Int = if (blocked) -1 else 0
        override fun inputBuffer(index: Int): ByteBuffer = bytes
        override fun queueInput(index: Int, size: Int, presentationUs: Long, end: Boolean) {
            queued.add(size to presentationUs)
            log.add("$name:input:$presentationUs:$end")
        }
        override fun output(): PlaybackCodecEvent = if (events.isEmpty()) PlaybackCodecEvent.Idle else events.removeFirst()
        override fun releaseOutput(index: Int, renderTimeNs: Long?) {
            releaseFailure?.let { throw it }
            log.add("$name:release:$index:$renderTimeNs")
        }
        override fun stop() { log.add("$name:stop") }
        override fun release() { log.add("$name:close") }
    }

    /** [clock] selects the video-only direct path: no synchronizer, display pacing on that clock. */
    private class Rig(audio: Boolean = true, units: Int = 4, bytes: Int = 32, clock: (() -> Long)? = null) {
        val log = mutableListOf<String>()
        val videoBackend = Codec("v", log)
        val audioBackend = Codec("a", log)
        val video = PlaybackCodec(videoBackend, false, 128)
        val audioCodec = if (audio) PlaybackCodec(audioBackend, true, 128) else null
        val pcm = mutableListOf<Triple<ByteBuffer, Int, Long>>()
        val sync = PlaybackSync(object : PlaybackSyncBackend {
            override fun queueAudio(bytes: ByteBuffer, token: Int, presentationUs: Long) { pcm.add(Triple(bytes, token, presentationUs)) }
            override fun flush() { log.add("sync:flush") }
            override fun release() { log.add("sync:release") }
        }, 1, 8, { audioCodec!!.release(it) }, { throw AssertionError("unexpected asynchronous failure", it) })
        val formats = mutableListOf<PlaybackOutputFormat>()
        val pump = PlaybackRenderPump(
            video, audioCodec, if (clock == null) sync else null, { formats.add(it) }, units, bytes,
            clock?.let { PlaybackVideoPacing(it) },
        )
        fun offer(lane: PlaybackLane, time: Long, syncUnit: Boolean = true, size: Int = 1): PlaybackOffer =
            pump.offer(lane, AccessUnitOutcome.Unit(ByteArray(size) { 1 }, time, syncUnit), time)
        fun start() {
            assertSame(PlaybackOffer.Queued, offer(PlaybackLane.Video, 20_000))
            if (audioCodec != null) assertSame(PlaybackOffer.Queued, offer(PlaybackLane.Audio, 0))
            pump.pump(1)
        }
        fun output(index: Int, time: Long, size: Int = 4, end: Boolean = false, config: Boolean = false) =
            PlaybackCodecEvent.Buffer(index, ByteBuffer.allocate(size), 0, size, time, end, config)
    }

    private val pcm = PlaybackOutputFormat("audio/raw", sampleRate = 48_000, channels = 2)

    /** The receive loop driving this real pump, exactly as the Android renderer delegates to it. */
    private fun port(r: Rig) = object : PlaybackRenderPort {
        override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long) = r.pump.offer(lane, unit, logicalUs)
        override fun pump() = r.pump.pump()
        override fun endInputs() = r.pump.endInputs()
        override val decoderOutputsEnded: Boolean get() = r.pump.decoderOutputsEnded
        override val videoOutputsReleased: Int get() = r.pump.videoReleased
    }
    private fun received(lane: PlaybackLane, us: Long) =
        PlaybackReceived(lane, listOf(PlaybackTimedUnit(AccessUnitOutcome.Unit(byteArrayOf(1), us, true), us)))

    @Test fun `released counts only successful nonempty non-config video output releases`() {
        val r = Rig()
        r.start()
        r.pump.pump(4)
        assertEquals(0, r.pump.videoReleased, "admitted input is not a released output")
        r.videoBackend.events.add(r.output(0, 20_000, config = true))
        r.videoBackend.events.add(PlaybackCodecEvent.Format(PlaybackOutputFormat("video/raw")))
        r.videoBackend.events.add(r.output(1, 20_000, size = 0))
        r.pump.pump(4)
        assertEquals(0, r.pump.videoReleased, "config, format and empty outputs are discarded, not rendered")
        assertTrue("v:release:0:null" in r.log && "v:release:1:null" in r.log)
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.output(2, 0))
        r.pump.pump(4)
        assertEquals(1, r.pcm.size)
        assertEquals(0, r.pump.videoReleased, "audio never counts")
        r.videoBackend.events.add(r.output(3, 20_000))
        r.videoBackend.events.add(r.output(4, 40_000))
        r.pump.pump(4)
        assertEquals(2, r.pump.videoReleased)
        r.pump.endInputs()
        r.pump.pump(2)
        r.videoBackend.events.add(r.output(5, 40_000, size = 0, end = true))
        r.audioBackend.events.add(r.output(6, 0, size = 0, end = true))
        r.pump.pump(2)
        assertTrue(r.pump.decoderOutputsEnded)
        assertEquals(2, r.pump.videoReleased, "an empty EOS buffer carries no frame")
    }

    @Test fun `a nonempty EOS video buffer is a rendered frame and counts once`() {
        val r = Rig(false)
        r.start()
        r.pump.endInputs()
        r.pump.pump(2)
        r.videoBackend.events.add(r.output(0, 20_000, size = 4, end = true))
        r.pump.pump(2)
        assertTrue("v:release:0:20000000" in r.log, "the pump renders media carried by an EOS buffer")
        assertEquals(1, r.pump.videoReleased)
    }

    @Test fun `a failed video release is never counted`() {
        val r = Rig(false)
        r.start()
        r.videoBackend.events.add(r.output(0, 20_000))
        r.pump.pump(1)
        assertEquals(1, r.pump.videoReleased)
        val boom = IllegalStateException("releaseOutputBuffer failed")
        r.videoBackend.releaseFailure = boom
        r.videoBackend.events.add(r.output(1, 40_000))
        assertSame(boom, runCatching { r.pump.pump(1) }.exceptionOrNull())
        assertEquals(1, r.pump.videoReleased)
    }

    @Test fun `object budget spent before selected audio starts is a named refusal not an unpresentable ObjectLimit`() = runTest {
        val r = Rig()
        val raised = runCatching {
            receivePlayback({ received(PlaybackLane.Video, 0) }, port(r), PlaybackFailure(), true, 2, waitLimitMs = 1_000)
        }.exceptionOrNull()
        assertTrue(raised is PlaybackRenderRefused, "got $raised")
        assertEquals("input ended before every selected track supplied startup media", raised.message)
        assertEquals(null, r.pump.originUs)
        assertFalse(r.log.any { it.endsWith(":true") }, "no decoder input EOS may be queued")
    }

    @Test fun `the same real pump completes ObjectLimit once every selected track started and both decoders reach EOS`() = runTest {
        val r = Rig()
        val packets = ArrayDeque(listOf(received(PlaybackLane.Video, 20_000), received(PlaybackLane.Audio, 0)))
        val result = async { receivePlayback({ packets.removeFirst() }, port(r), PlaybackFailure(), true, 2, waitLimitMs = 1_000) }
        runCurrent(); advanceTimeBy(10); runCurrent()
        assertFalse(result.isCompleted, "no success before decoder output EOS")
        assertEquals(2, r.log.count { it.endsWith(":true") })
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.output(1, 0, end = true))
        r.videoBackend.events.add(r.output(2, 20_000, end = true))
        advanceTimeBy(5); runCurrent()
        assertEquals(PlaybackReadResult(PlaybackReadEnd.ObjectLimit, 2, 2), withTimeout(100) { result.await() })
        assertTrue(r.pump.decoderOutputsEnded)
    }

    @Test fun `startup waits for both tracks and freezes one minimum preserving negative offsets`() {
        val r = Rig()
        r.offer(PlaybackLane.Video, 0)
        assertFalse(r.pump.pump(4))
        assertTrue(r.videoBackend.queued.isEmpty())
        r.offer(PlaybackLane.Audio, -20_000)
        assertTrue(r.pump.pump(1))
        assertEquals(-20_000, r.pump.originUs)
        assertEquals(listOf(1 to 20_000L), r.videoBackend.queued)
        assertEquals(listOf(1 to 0L), r.audioBackend.queued)
        assertEquals(listOf("v:input:20000:false", "a:input:0:false"), r.log)
    }

    @Test fun `startup origin includes all retained media not just the first sample per track`() {
        val r = Rig()
        r.offer(PlaybackLane.Video, 10)
        r.offer(PlaybackLane.Video, -10)
        r.offer(PlaybackLane.Audio, 0)
        r.pump.pump(2)
        assertEquals(-10, r.pump.originUs)
        assertEquals(listOf(1 to 20L, 1 to 0L), r.videoBackend.queued)
        assertEquals(listOf(1 to 10L), r.audioBackend.queued)
    }

    @Test fun `leading non-sync video is accounted without becoming a startup anchor`() {
        val r = Rig(false)
        assertSame(PlaybackOffer.DiscardedUntilSync, r.offer(PlaybackLane.Video, -999, false))
        assertEquals(1L, r.pump.discardedBeforeSync)
        assertFalse(r.pump.pump(1))
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 50))
        r.pump.pump(1)
        assertEquals(50, r.pump.originUs)
        assertEquals(listOf(1 to 0L), r.videoBackend.queued)
    }

    @Test fun `startup capacity fails by name rather than blocking the missing audio behind video`() {
        val r = Rig(units = 1)
        r.offer(PlaybackLane.Video, 0)
        val failure = assertFailsWith<PlaybackRenderRefused> { r.offer(PlaybackLane.Video, 1) }
        assertTrue(failure.message!!.contains("startup"))
        assertTrue(r.videoBackend.queued.isEmpty())
    }

    @Test fun `post-start capacity returns busy and retry never duplicates`() {
        val r = Rig(false, units = 1, bytes = 3)
        r.start()
        r.videoBackend.blocked = true
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 20_001, size = 2))
        assertSame(PlaybackOffer.Busy, r.offer(PlaybackLane.Video, 20_002, size = 2))
        assertFalse(r.pump.pump(2))
        r.videoBackend.blocked = false
        r.pump.pump(1)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 20_002, size = 2))
        r.pump.pump(1)
        assertEquals(listOf(1 to 0L, 2 to 1L, 2 to 2L), r.videoBackend.queued)
    }

    @Test fun `late earlier input and unrepresentable span refuse before codec calls without moving origin`() {
        val r = Rig(false)
        r.start()
        assertFailsWith<PlaybackRenderRefused> { r.offer(PlaybackLane.Video, 19_999) }
        assertEquals(20_000, r.pump.originUs)
        val overflow = Rig()
        overflow.offer(PlaybackLane.Video, Long.MIN_VALUE)
        assertFailsWith<PlaybackRenderRefused> { overflow.offer(PlaybackLane.Audio, Long.MAX_VALUE) }
        assertTrue(overflow.videoBackend.queued.isEmpty())
        val huge = Rig(false)
        huge.offer(PlaybackLane.Video, 0)
        huge.pump.pump(1)
        assertFailsWith<PlaybackRenderRefused> { huge.offer(PlaybackLane.Video, Long.MAX_VALUE / 1000 + 1) }
    }

    @Test fun `signed extreme equal timestamps map safely without negating origin`() {
        val r = Rig()
        r.offer(PlaybackLane.Video, Long.MIN_VALUE)
        r.offer(PlaybackLane.Audio, Long.MIN_VALUE)
        r.pump.pump(1)
        assertEquals(listOf(1 to 0L), r.videoBackend.queued)
        assertEquals(listOf(1 to 0L), r.audioBackend.queued)
    }

    @Test fun `audio capacity does not block video and PCM releases only through exact consumption`() {
        val r = Rig()
        r.start()
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.output(1, 0))
        r.audioBackend.events.add(r.output(2, 100))
        r.pump.pump(4)
        assertEquals(listOf(pcm), r.formats)
        assertEquals(1, r.pcm.size)
        assertTrue(r.log.none { it.startsWith("a:release") })
        r.videoBackend.events.add(r.output(4, 20_000))
        r.pump.pump(1)
        assertTrue("v:release:4:20000000" in r.log)
        val first = r.pcm.single()
        r.sync.onAudioConsumed(first.second, first.first)
        r.pump.pump(1)
        assertEquals(2, r.pcm.size)
        assertEquals(listOf("a:release:1:null"), r.log.filter { it.startsWith("a:release") })
    }

    @Test fun `PCM before a format and changed PCM format stop instead of reusing a mismatched track`() {
        val r = Rig()
        r.start()
        r.audioBackend.events.add(r.output(0, 0))
        assertFailsWith<PlaybackRenderRefused> { r.pump.pump(1) }
        assertTrue(r.pcm.isEmpty())
        val changed = Rig()
        changed.start()
        changed.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        changed.audioBackend.events.add(PlaybackCodecEvent.Format(pcm.copy(sampleRate = 44_100)))
        assertFailsWith<PlaybackRenderRefused> { changed.pump.pump(2) }
        assertEquals(listOf(pcm), changed.formats)
    }

    @Test fun `equal formats configure once and empty config outputs never become PCM or video`() {
        val r = Rig()
        r.start()
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.output(1, 0, config = true))
        r.audioBackend.events.add(r.output(2, 0, size = 0))
        r.videoBackend.events.add(r.output(3, 0, config = true))
        r.pump.pump(8)
        assertEquals(listOf(pcm), r.formats)
        assertTrue(r.pcm.isEmpty())
        assertTrue("v:release:3:null" in r.log)
        assertEquals(listOf("a:release:1:null", "a:release:2:null"), r.log.filter { it.startsWith("a:release") })
    }

    @Test fun `only explicit input exhaustion queues EOS and nonempty output EOS still carries media`() {
        val r = Rig()
        r.start()
        r.pump.pump(4)
        assertFalse(r.log.any { it.endsWith(":true") })
        r.pump.endInputs()
        r.pump.pump(3)
        assertEquals(2, r.log.count { it.endsWith(":true") })
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.output(1, 0, end = true))
        r.videoBackend.events.add(r.output(2, 20_000, end = true))
        r.pump.pump(3)
        assertTrue(r.pump.decoderOutputsEnded)
        assertEquals(1, r.pcm.size)
        assertTrue("v:release:2:20000000" in r.log)
        assertFailsWith<IllegalStateException> { r.offer(PlaybackLane.Video, 30_000) }
    }

    @Test fun `one round bounds work on each codec and stop rejects later input without platform calls`() {
        val r = Rig(false)
        r.offer(PlaybackLane.Video, 0)
        r.offer(PlaybackLane.Video, 1)
        r.pump.pump(1)
        assertEquals(1, r.videoBackend.queued.size)
        val log = r.log.toList()
        r.pump.stop()
        r.pump.stop()
        assertFailsWith<IllegalStateException> { r.pump.pump(1) }
        assertFailsWith<IllegalStateException> { r.offer(PlaybackLane.Video, 2) }
        assertEquals(log, r.log)
    }

    // -- the video-only direct path: paced release on the monotonic clock ------------------------

    private class Clock(var now: Long = 7_000_000_000) { fun read() = now }

    private fun releases(r: Rig) = r.log.filter { it.startsWith("v:release:") }

    @Test fun `direct frames are released one media interval apart on the fake clock, stamped in its domain`() {
        val clock = Clock()
        val r = Rig(false, units = 8, clock = clock::read)
        r.start()
        r.videoBackend.events.add(r.output(0, 0))
        r.videoBackend.events.add(r.output(1, 41_666))
        r.videoBackend.events.add(r.output(2, 83_332))
        val released = mutableListOf<Long>()
        fun step(advanceNs: Long) {
            clock.now += advanceNs
            val before = releases(r).size
            r.pump.pump(1)
            if (releases(r).size > before) released += clock.now
        }
        step(0)
        assertEquals(listOf("v:release:0:7000000000"), releases(r), "the first frame anchors and is due at once")
        step(40_000_000)
        assertEquals(1, releases(r).size, "a frame is not released before its deadline")
        step(1_666_000)
        step(41_666_000)
        assertEquals(
            listOf("v:release:0:7000000000", "v:release:1:7041666000", "v:release:2:7083332000"), releases(r),
            "each stamp is the monotonic deadline, never the relative media time",
        )
        assertEquals(listOf(7_000_000_000, 7_041_666_000, 7_083_332_000), released, "release instants are spaced by the media interval")
        assertEquals(3, r.pump.videoReleased)
    }

    @Test fun `a held future frame returns control and input keeps flowing to the decoder`() {
        val clock = Clock()
        val r = Rig(false, units = 8, clock = clock::read)
        r.start()
        r.videoBackend.events.add(r.output(0, 0))
        r.videoBackend.events.add(r.output(1, 1_000_000))
        r.videoBackend.events.add(r.output(2, 1_041_666))
        r.pump.pump(1)
        r.pump.pump(1)
        assertEquals(1, releases(r).size, "the second frame is a second away and stays held")
        val admitted = r.videoBackend.queued.size
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 60_000))
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 100_000))
        r.pump.pump(4)
        assertEquals(admitted + 2, r.videoBackend.queued.size, "held output does not stop input admission")
        assertEquals(1, releases(r).size)
        assertEquals(1, r.videoBackend.events.size, "no later output is taken while an earlier one is held")
        clock.now += 1_000_000_000
        r.pump.pump(1)
        assertEquals("v:release:1:8000000000", releases(r).last(), "order is preserved: the held frame goes first")
    }

    @Test fun `a late frame is released at once and none is dropped`() {
        val clock = Clock()
        val r = Rig(false, units = 8, clock = clock::read)
        r.start()
        r.videoBackend.events.add(r.output(0, 0))
        r.pump.pump(1)
        clock.now += 500_000_000
        r.videoBackend.events.add(r.output(1, 41_666))
        r.videoBackend.events.add(r.output(2, 83_332))
        r.pump.pump(2)
        assertEquals(
            listOf("v:release:0:7000000000", "v:release:1:7041666000", "v:release:2:7083332000"), releases(r),
            "past deadlines release immediately and keep their monotonic stamps",
        )
        assertEquals(3, r.pump.videoReleased)
    }

    @Test fun `config and empty buffers never render, never anchor, and an empty EOS ends at once`() {
        val clock = Clock()
        val r = Rig(false, units = 8, clock = clock::read)
        r.start()
        r.videoBackend.events.add(r.output(0, 0, config = true))
        r.videoBackend.events.add(r.output(1, 0, size = 0))
        r.pump.pump(2)
        assertEquals(listOf("v:release:0:null", "v:release:1:null"), releases(r))
        clock.now += 3_000_000_000
        r.videoBackend.events.add(r.output(2, 41_666))
        r.pump.pump(1)
        assertEquals("v:release:2:10000000000", releases(r).last(), "the first rendered frame anchors, not a discarded one")
        r.pump.endInputs()
        r.pump.pump(1)
        r.videoBackend.events.add(r.output(3, 41_666, size = 0, end = true))
        r.pump.pump(1)
        assertEquals("v:release:3:null", releases(r).last())
        assertTrue(r.pump.decoderOutputsEnded)
        assertEquals(1, r.pump.videoReleased)
    }

    @Test fun `a nonempty EOS frame ends the decoder only once it is released at its deadline`() {
        val clock = Clock()
        val r = Rig(false, units = 8, clock = clock::read)
        r.start()
        r.videoBackend.events.add(r.output(0, 0))
        r.pump.pump(1)
        r.pump.endInputs()
        r.pump.pump(1)
        r.videoBackend.events.add(r.output(1, 41_666, end = true))
        r.pump.pump(1)
        assertFalse(r.pump.decoderOutputsEnded, "decoder EOS is not reported while its frame is still held")
        clock.now += 41_666_000
        r.pump.pump(1)
        assertTrue(r.pump.decoderOutputsEnded)
        assertEquals(2, r.pump.videoReleased)
    }

    @Test fun `stop drops a held frame to its codec, whose close settles it exactly once`() {
        val clock = Clock()
        val r = Rig(false, units = 8, clock = clock::read)
        r.start()
        r.videoBackend.events.add(r.output(0, 0))
        r.videoBackend.events.add(r.output(1, 41_666))
        r.pump.pump(2)
        assertEquals(1, releases(r).size)
        r.pump.stop()
        clock.now += 1_000_000_000
        assertFailsWith<IllegalStateException> { r.pump.pump(1) }
        r.video.close()
        r.video.close()
        assertEquals(listOf("v:release:0:7000000000"), releases(r), "the held frame is never rendered after stop")
        assertEquals(1, r.log.count { it == "v:stop" })
        assertEquals(1, r.log.count { it == "v:close" })
        assertEquals(1, r.pump.videoReleased)
    }

    @Test fun `a failed paced release is not counted and leaves the frame with its codec`() {
        val clock = Clock()
        val r = Rig(false, units = 8, clock = clock::read)
        r.start()
        r.videoBackend.releaseFailure = IllegalStateException("surface abandoned")
        r.videoBackend.events.add(r.output(0, 0))
        assertFailsWith<IllegalStateException> { r.pump.pump(1) }
        assertEquals(0, r.pump.videoReleased)
        assertFailsWith<IllegalStateException> { r.pump.pump(1) }
        r.pump.stop()
        r.video.close()
        assertEquals(1, r.log.count { it == "v:close" })
    }

    @Test fun `the direct path is video only and every path has a video destination`() {
        val r = Rig(true)
        assertFailsWith<IllegalArgumentException> {
            PlaybackRenderPump(r.video, r.audioCodec, null, {}, 4, 32, PlaybackVideoPacing { 0 })
        }
        assertFailsWith<IllegalArgumentException> { PlaybackRenderPump(r.video, null, null, {}, 4, 32, null) }
        assertFailsWith<IllegalArgumentException> { PlaybackRenderPump(r.video, r.audioCodec, null, {}, 4, 32, null) }
    }
}
