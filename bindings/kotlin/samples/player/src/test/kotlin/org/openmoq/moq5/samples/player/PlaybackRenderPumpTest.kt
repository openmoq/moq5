package org.openmoq.moq5.samples.player

import java.nio.ByteBuffer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
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

    /** An AudioTrack that takes [room] bytes, has consumed [head] frames, and reports [stamp]. */
    private class Sink(val log: MutableList<String>) : PlaybackAudioSink {
        var room = Int.MAX_VALUE
        var head = 0
        var stamp: PlaybackAudioTimestamp? = null
        val written = mutableListOf<Int>()
        override fun write(bytes: ByteBuffer): Int {
            val n = minOf(bytes.remaining(), room)
            bytes.position(bytes.position() + n)
            if (room != Int.MAX_VALUE) room -= n
            written += n
            return n
        }
        override fun head(): Int = head
        override fun timestamp(): PlaybackAudioTimestamp? = stamp
        override fun play() { log.add("sink:play") }
    }

    private class Clock(var now: Long = 7_000_000_000) { fun read() = now }

    /** With [audio], video follows the audio output's clock; without, its own display clock. Both on [clock]. */
    private class Rig(audio: Boolean = true, units: Int = 4, bytes: Int = 32, startup: Int? = null) {
        val log = mutableListOf<String>()
        val clock = Clock()
        val videoBackend = Codec("v", log)
        val audioBackend = Codec("a", log)
        val video = PlaybackCodec(videoBackend, false, 128)
        val audioCodec = if (audio) PlaybackCodec(audioBackend, true, 128) else null
        val sink = Sink(log)
        val formats = mutableListOf<PlaybackOutputFormat>()
        val notices = mutableListOf<String>()
        val out = if (audio) PlaybackAudioOutput({ formats.add(it); PlaybackAudioOpened(sink, 48_000, 4) }, clock::read, { notices.add(it) }) else null
        val pacing = PlaybackVideoPacing(clock::read, out)
        val pump =
            if (startup == null) PlaybackRenderPump(video, audioCodec, out, pacing, units, bytes)
            else PlaybackRenderPump(video, audioCodec, out, pacing, units, bytes, startup)
        fun offer(lane: PlaybackLane, time: Long, syncUnit: Boolean = true, size: Int = 1, group: ULong? = null): PlaybackOffer =
            pump.offer(lane, AccessUnitOutcome.Unit(ByteArray(size) { 1 }, time, syncUnit), time, group)
        fun start() {
            assertSame(PlaybackOffer.Queued, offer(PlaybackLane.Video, 20_000))
            if (audioCodec != null) assertSame(PlaybackOffer.Queued, offer(PlaybackLane.Audio, 0))
            pump.pump(1)
        }
        fun output(index: Int, time: Long, size: Int = 4, end: Boolean = false, config: Boolean = false) =
            PlaybackCodecEvent.Buffer(index, ByteBuffer.allocate(size), 0, size, time, end, config)
        /** Stereo PCM16 at 48 kHz: [frames] frames from [time]. */
        fun pcm(index: Int, time: Long, frames: Int, end: Boolean = false) = output(index, time, frames * 4, end)
    }

    private val pcm = PlaybackOutputFormat("audio/raw", sampleRate = 48_000, channels = 2)

    /** The receive loop driving this real pump, exactly as the Android renderer delegates to it. */
    private fun port(r: Rig) = object : PlaybackRenderPort {
        override fun offer(lane: PlaybackLane, unit: AccessUnitOutcome.Unit, logicalUs: Long, group: ULong?) = r.pump.offer(lane, unit, logicalUs, group)
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
        r.audioBackend.events.add(r.pcm(2, 0, 4_800))
        r.pump.pump(4)
        assertEquals(listOf(19_200), r.sink.written)
        assertEquals(0, r.pump.videoReleased, "audio never counts")
        r.sink.head = 1_920
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
        assertTrue("v:release:0:7000000000" in r.log, "the pump renders media carried by an EOS buffer")
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
        r.clock.now += 20_000_000
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

    // -- startup: a relay may deliver one track's multi-group backlog before the other's first object ------

    /** [per] units of each of [groups], interleaved one unit per group at a time; a group a second, units [stepUs] apart. */
    private fun interleaved(groups: List<ULong>, per: Int, stepUs: Long): List<Pair<Long, ULong>> =
        (0 until per).flatMap { k -> groups.map { g -> ((g - groups.first()).toLong() * 1_000_000 + k * stepUs) to g } }

    /** Drain both lanes, letting a waiting last group start on its quiet bound. */
    private fun drain(r: Rig) {
        repeat(20) { r.pump.pump(8) }
        r.clock.now += 500_000_000
        repeat(20) { r.pump.pump(8) }
    }

    @Test fun `an audio backlog of interleaved groups beyond the steady bound waits for video, then decodes whole and in group order`() {
        val r = Rig(units = 64, bytes = 1 shl 20)
        val audio = interleaved(listOf(8uL, 9uL, 10uL, 11uL), 20, 21_333)
        for ((t, g) in audio) assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Audio, t, group = g), "audio of group $g at $t us")
        assertEquals(null, r.pump.originUs, "nothing starts before the video track supplies media")
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 500_000, group = 0uL))
        assertEquals(0L, r.pump.originUs, "one common minimum: audio group 8's first unit")
        drain(r)
        assertEquals(
            audio.sortedWith(compareBy({ it.second }, { it.first })).map { it.first },
            r.audioBackend.queued.map { it.second },
            "all 80 audio units reach the decoder, group by group",
        )
        assertEquals(listOf(500_000L), r.videoBackend.queued.map { it.second })
        assertEquals(listOf(0L, 0L, 0L), listOf(r.pump.discardedLateGroup, r.pump.skippedLate, r.pump.discardedBeforeSync), "nothing discarded")
    }

    @Test fun `a video backlog of interleaved groups beyond the steady bound waits for audio, then decodes whole and in group order`() {
        val r = Rig(units = 64, bytes = 1 shl 20)
        val video = interleaved(listOf(0uL, 1uL, 2uL), 24, 41_708)
        for ((t, g) in video) {
            val keyframe = t == g.toLong() * 1_000_000
            assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, t, syncUnit = keyframe, group = g), "video of group $g at $t us")
        }
        assertEquals(null, r.pump.originUs, "nothing starts before the audio track supplies media")
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Audio, 1_000_000, group = 5uL))
        assertEquals(0L, r.pump.originUs, "one common minimum: video group 0's keyframe")
        drain(r)
        assertEquals(
            video.sortedWith(compareBy({ it.second }, { it.first })).map { it.first },
            r.videoBackend.queued.map { it.second },
            "all 72 video units reach the decoder, group by group",
        )
        assertEquals(listOf(1_000_000L), r.audioBackend.queued.map { it.second })
        assertEquals(listOf(0L, 0L, 0L), listOf(r.pump.discardedLateGroup, r.pump.skippedLate, r.pump.discardedBeforeSync), "nothing discarded")
    }

    @Test fun `startup capacity fails by name rather than blocking the missing audio behind video`() {
        val r = Rig(units = 1, startup = 1)
        r.offer(PlaybackLane.Video, 0)
        val failure = assertFailsWith<PlaybackRenderRefused> { r.offer(PlaybackLane.Video, 1) }
        assertEquals("startup media exceeded its entry bound before all selected tracks arrived", failure.message)
        assertTrue(r.videoBackend.queued.isEmpty())
    }

    @Test fun `startup entries are bounded below the payload cap, the bound admitted and one more failing by name`() {
        val r = Rig(units = 4, bytes = 1 shl 20, startup = 8)
        repeat(8) { assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, it * 1_000L), "one-byte unit $it of 8") }
        val failure = assertFailsWith<PlaybackRenderRefused> { r.offer(PlaybackLane.Video, 8_000) }
        assertEquals("startup media exceeded its entry bound before all selected tracks arrived", failure.message, "8 bytes of a 1 MiB cap")
        assertEquals(null, r.pump.originUs)
        assertTrue(r.videoBackend.queued.isEmpty())
    }

    @Test fun `startup payload is bounded independently of entries and fails by its own name`() {
        val r = Rig(units = 4, bytes = 32, startup = 8)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 0, size = 16))
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 1_000, size = 16))
        val failure = assertFailsWith<PlaybackRenderRefused> { r.offer(PlaybackLane.Video, 2_000, size = 1) }
        assertEquals("startup media exceeded its payload bound before all selected tracks arrived", failure.message, "two entries of eight")
        assertTrue(r.videoBackend.queued.isEmpty())
    }

    @Test fun `an absent track still ends startup at the receive loop's deadline, through the real pump`() = runTest {
        val r = Rig(units = 4, bytes = 1 shl 20, startup = 8)
        var reads = 0
        val result = async {
            runCatching {
                receivePlayback({ if (reads < 3) received(PlaybackLane.Video, 20_000L * reads++) else awaitCancellation() },
                    port(r), PlaybackFailure(), true, 100, waitLimitMs = 10_000)
            }
        }
        runCurrent()
        advanceTimeBy(9_999); runCurrent()
        assertFalse(result.isCompleted, "three video units within both bounds: still waiting for audio")
        advanceTimeBy(1); runCurrent()
        val failure = withTimeout(100) { result.await() }.exceptionOrNull()
        assertTrue(failure is PlaybackRenderRefused, "got $failure")
        assertEquals("selected tracks did not supply startup media within the sample's wait bound", failure.message)
        assertEquals(null, r.pump.originUs)
        assertTrue(r.videoBackend.queued.isEmpty())
    }

    @Test fun `after startup a prefix beyond the steady bound drains through Busy without loss or duplication`() {
        val r = Rig(units = 4, bytes = 1 shl 20, startup = 16)
        for (i in 0 until 10) assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 20_000L + i * 1_000))
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Audio, 0))
        assertEquals(0L, r.pump.originUs)
        assertSame(PlaybackOffer.Busy, r.offer(PlaybackLane.Video, 30_000), "ten queued: the steady bound of four applies once started")
        r.pump.pump(1)
        assertSame(PlaybackOffer.Busy, r.offer(PlaybackLane.Video, 30_000), "nine queued")
        repeat(6) { r.pump.pump(1) }
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 30_000), "three queued: the retried unit is admitted once")
        repeat(8) { r.pump.pump(1) }
        assertEquals((0 until 10).map { 20_000L + it * 1_000 } + 30_000L, r.videoBackend.queued.map { it.second })
        assertEquals(listOf(0L), r.audioBackend.queued.map { it.second })
    }

    @Test fun `stop during startup drops the retained prefix and admits nothing after`() {
        val r = Rig(units = 4, bytes = 1 shl 20, startup = 16)
        repeat(10) { assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, it * 1_000L)) }
        r.pump.stop()
        assertFailsWith<IllegalStateException> { r.offer(PlaybackLane.Audio, 0) }
        assertFailsWith<IllegalStateException> { r.pump.pump(1) }
        assertTrue(r.videoBackend.queued.isEmpty() && r.audioBackend.queued.isEmpty(), "nothing retained reached a decoder")
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

    @Test fun `a full sink holds its PCM output without blocking video, and returns it once all of it is written`() {
        val r = Rig()
        r.start()
        r.sink.room = 8
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.pcm(1, 0, 4))
        r.audioBackend.events.add(r.pcm(2, 83, 4))
        r.pump.pump(4)
        assertEquals(listOf(pcm), r.formats)
        assertEquals(1, r.log.count { it == "sink:play" }, "the track starts playing once, when it opens")
        assertEquals(8, r.sink.written.sum())
        assertTrue(r.log.none { it.startsWith("a:release") }, "a partly written output stays with its decoder")
        r.videoBackend.events.add(r.output(4, 20_000))
        r.pump.pump(1)
        assertTrue("v:release:4:7000000000" in r.log, "video is not blocked behind audio")
        r.sink.room = Int.MAX_VALUE
        r.pump.pump(1)
        assertEquals(listOf("a:release:1:null"), r.log.filter { it.startsWith("a:release") })
        assertEquals(16, r.sink.written.sum())
        assertEquals(1, r.audioBackend.events.size, "the next output waits for the held one")
    }

    @Test fun `PCM before a format and changed PCM format stop instead of reusing a mismatched track`() {
        val r = Rig()
        r.start()
        r.audioBackend.events.add(r.output(0, 0))
        assertFailsWith<PlaybackRenderRefused> { r.pump.pump(1) }
        assertTrue(r.sink.written.isEmpty())
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
        assertTrue(r.sink.written.isEmpty())
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
        assertEquals(listOf(4), r.sink.written)
        assertTrue("v:release:2:7000000000" in r.log, "before audio is heard the first frame shows at once")
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

    // -- the video-only path: paced release on the monotonic display clock -----------------------

    private fun releases(r: Rig) = r.log.filter { it.startsWith("v:release:") }

    @Test fun `direct frames are released one media interval apart on the fake clock, stamped in its domain`() {
        val r = Rig(false, units = 8)
        val clock = r.clock
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
        val r = Rig(false, units = 8)
        val clock = r.clock
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
        val r = Rig(false, units = 8)
        val clock = r.clock
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
        val r = Rig(false, units = 8)
        val clock = r.clock
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
        val r = Rig(false, units = 8)
        val clock = r.clock
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
        val r = Rig(false, units = 8)
        val clock = r.clock
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
        val r = Rig(false, units = 8)
        val clock = r.clock
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

    @Test fun `audio is decoded exactly when it has an output`() {
        val r = Rig(true)
        val pacing = PlaybackVideoPacing({ 0L })
        assertFailsWith<IllegalArgumentException> { PlaybackRenderPump(r.video, r.audioCodec, null, pacing, 4, 32) }
        assertFailsWith<IllegalArgumentException> { PlaybackRenderPump(r.video, null, r.out, pacing, 4, 32) }
    }

    // -- A/V: video follows the audio being heard ---------------------------------------------

    @Test fun `video follows the audio being heard, and the first frame shows before audio starts`() {
        val r = Rig(units = 8)
        r.start()
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.pcm(1, 0, 4_800))
        r.videoBackend.events.add(r.output(2, 20_000))
        r.videoBackend.events.add(r.output(3, 40_000))
        r.pump.pump(4)
        assertEquals(listOf("v:release:2:7000000000"), releases(r), "nothing heard yet: one frame shows, the next waits")
        r.clock.now += 10_000_000
        r.sink.head = 960
        r.pump.pump(2)
        assertEquals(1, releases(r).size, "audio at 20 ms; the 40 ms frame waits")
        r.clock.now += 20_000_000
        r.sink.head = 1_920
        r.pump.pump(1)
        assertEquals("v:release:3:7030000000", releases(r).last(), "released when the audio reaches it, stamped now")
        assertTrue(r.out!!.leading)
        assertEquals(emptyList(), r.notices)
    }

    @Test fun `a frame more than the bound behind the audio is dropped and not counted, but never the first`() {
        val r = Rig(units = 8)
        r.start()
        r.sink.head = 9_600
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.pcm(1, 0, 48_000))
        r.videoBackend.events.add(r.output(2, 20_000))
        r.videoBackend.events.add(r.output(3, 40_000))
        r.videoBackend.events.add(r.output(4, 160_000))
        r.videoBackend.events.add(r.output(5, 220_000))
        r.pump.pump(8)
        assertEquals(listOf("v:release:2:7000000000", "v:release:3:null", "v:release:4:7000000000"), releases(r))
        assertEquals(2, r.pump.videoReleased)
        assertEquals(1L, r.pump.videoDroppedLate, "160 ms late at 200 ms of audio; 40 ms late is shown")
        assertTrue(releases(r).none { it.startsWith("v:release:5:") }, "the 220 ms frame is early and held")
    }

    // -- groups arrive interleaved (one QUIC stream each); each lane decodes them in order --------

    @Test fun `a late tail of the group being decoded goes before the next group, which waits for the clock`() {
        val r = Rig(false, units = 8)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 0, group = 1uL))
        r.pump.pump(1)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 2_000_000, group = 2uL), "group 2's first unit overtakes group 1's tail")
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 40_000, group = 1uL))
        r.pump.pump(4)
        assertEquals(listOf(1 to 0L, 1 to 40_000L), r.videoBackend.queued, "the tail is decoded first; group 2 waits, nothing presented yet")
        r.videoBackend.events.add(r.output(5, 1_499_999))
        r.pump.pump(2)
        assertEquals(2, r.videoBackend.queued.size, "presented at 1.499999 s: still more than 0.5 s before group 2")
        r.videoBackend.events.add(r.output(6, 1_500_000))
        r.clock.now += 1_000
        r.pump.pump(2)
        assertEquals(1 to 2_000_000L, r.videoBackend.queued.last(), "the clock is within 0.5 s of group 2: it starts")
        assertEquals(1L, r.pump.groupAdvances)
        assertSame(PlaybackOffer.DiscardedLateGroup, r.offer(PlaybackLane.Video, 80_000, group = 1uL), "group 1 is passed")
        assertEquals(1L, r.pump.discardedLateGroup)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 2_040_000, group = 2uL))
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 2_080_000), "a unit without a group keeps its arrival order")
    }

    @Test fun `with no clock, a waiting group starts once a second of later media is queued behind it`() {
        val r = Rig(false, units = 8)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 0, group = 1uL))
        r.pump.pump(1)
        for (t in listOf(1_000_000L, 1_500_000L, 1_999_999L)) assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, t, group = 2uL))
        r.pump.pump(4)
        assertEquals(listOf(1 to 0L), r.videoBackend.queued, "0.999999 s queued behind group 2's first unit: it waits")
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 2_000_000, group = 2uL))
        r.pump.pump(3)
        assertEquals(listOf(0L, 1_000_000L, 1_500_000L, 1_999_999L), r.videoBackend.queued.map { it.second }, "a second queued: group 1's tail is given up")
        assertEquals(1L, r.pump.groupAdvances)
    }

    @Test fun `with no clock, a full input starts the waiting group`() {
        val r = Rig(false, units = 3)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 0, group = 1uL))
        r.pump.pump(1)
        for (t in listOf(100_000L, 140_000L)) assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, t, group = 2uL))
        r.pump.pump(2)
        assertEquals(1, r.videoBackend.queued.size, "two queued: not yet")
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 180_000, group = 2uL))
        r.pump.pump(1)
        assertEquals(1 to 100_000L, r.videoBackend.queued.last(), "a full input stops waiting for group 1")
    }

    @Test fun `once a later group is due, the rest of the group being decoded is skipped undecoded`() {
        val r = Rig(false, units = 8)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 0, group = 0uL))
        r.pump.pump(1)
        r.videoBackend.blocked = true
        // A cached backlog of group 0, then the live group 1.
        for (t in listOf(40_000L, 80_000L)) assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, t, group = 0uL))
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 1_000_000, group = 1uL))
        r.videoBackend.events.add(r.output(5, 1_000_000))
        r.pump.pump(1)
        assertEquals(2L, r.pump.skippedLate, "the clock is at group 1's first unit: group 0's queued rest could only be late")
        r.videoBackend.blocked = false
        r.pump.pump(1)
        assertEquals(listOf(0L, 1_000_000L), r.videoBackend.queued.map { it.second }, "group 0's backlog never reached the decoder")
        assertSame(PlaybackOffer.DiscardedLateGroup, r.offer(PlaybackLane.Video, 120_000, group = 0uL))
    }

    @Test fun `two groups delivered interleaved are decoded group by group, as the phone saw from the relay`() {
        val r = Rig(units = 8)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 6_000_000, group = 0uL))
        // Audio objects of groups 8 and 7, in the order they arrived on the device.
        for ((t, g) in listOf(7_040_000L to 8uL, 6_037_333L to 7uL, 6_058_666L to 7uL, 7_061_333L to 8uL, 6_080_000L to 7uL)) {
            assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Audio, t, group = g))
        }
        r.pump.pump(8)
        assertEquals(listOf(37_333L, 58_666L, 80_000L), r.audioBackend.queued.map { it.second }, "group 7 in order; group 8 waits behind it")
        assertEquals(0L, r.pump.discardedLateGroup, "nothing arrived after its group was passed")
    }

    @Test fun `a short final group across a timestamp gap is decoded once input is exhausted, and still shows on its own time`() {
        val r = Rig(false, units = 8, bytes = 64)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 0, group = 1uL))
        r.pump.pump(1)
        r.videoBackend.events.add(r.output(5, 0))
        r.pump.pump(1)
        val t0 = r.clock.now
        assertEquals(listOf("v:release:5:$t0"), releases(r), "group 1 is shown: the presented media clock is 0")
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 2_000_000, group = 2uL), "one unit of a short final group, 2 s on")
        r.pump.endInputs()
        r.pump.pump(4)
        assertEquals(
            listOf("v:input:0:false", "v:input:2000000:false", "v:input:2000000:true"),
            r.log.filter { it.startsWith("v:input:") },
            "exhausted input has no group-1 tail left to wait for: group 2 is decoded at once, then the input ends",
        )
        assertEquals(1L, r.pump.groupAdvances)
        r.videoBackend.events.add(r.output(6, 2_000_000, end = true))
        r.clock.now = t0 + 1_999_999_999
        r.pump.pump(1)
        assertEquals(1, releases(r).size, "decoded early, it waits for its display deadline")
        assertFalse(r.pump.decoderOutputsEnded)
        r.clock.now = t0 + 2_000_000_000
        r.pump.pump(1)
        assertEquals("v:release:6:${t0 + 2_000_000_000}", releases(r).last())
        assertTrue(r.pump.decoderOutputsEnded)
    }

    @Test fun `a sparse live group across a timestamp gap starts after a bounded quiet wait, which a late tail restarts`() {
        val r = Rig(false, units = 8, bytes = 64)
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 0, group = 1uL))
        r.pump.pump(1)
        r.videoBackend.events.add(r.output(5, 0))
        r.pump.pump(1)
        val t0 = r.clock.now
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 2_000_000, group = 2uL), "a lone unit of the next group, 2 s on")
        r.pump.pump(2)
        r.clock.now = t0 + 499_999_999
        r.pump.pump(2)
        assertEquals(listOf(0L), r.videoBackend.queued.map { it.second }, "group 1's tail may still be on its way")
        assertSame(PlaybackOffer.Queued, r.offer(PlaybackLane.Video, 40_000, group = 1uL), "and it comes")
        r.pump.pump(2)
        assertEquals(listOf(0L, 40_000L), r.videoBackend.queued.map { it.second }, "the tail goes first, and group 2 waits afresh")
        r.clock.now = t0 + 999_999_998
        r.pump.pump(2)
        assertEquals(2, r.videoBackend.queued.size, "nothing presented moves the media clock across the gap")
        r.clock.now = t0 + 999_999_999
        r.pump.pump(2)
        assertEquals(listOf(0L, 40_000L, 2_000_000L), r.videoBackend.queued.map { it.second }, "group 1 has been quiet for the wait bound: group 2 starts")
        assertEquals(1L, r.pump.groupAdvances)
        r.videoBackend.events.add(r.output(6, 2_000_000))
        r.pump.pump(1)
        assertEquals(1, releases(r).size, "starting the group decodes it early; nothing is shown before its time")
        r.clock.now = t0 + 2_000_000_000
        r.pump.pump(1)
        assertEquals("v:release:6:${t0 + 2_000_000_000}", releases(r).last())
    }

    @Test fun `a stalled sink hands video to the display clock, says so once, and drops PCM instead of backing up`() {
        val r = Rig(units = 8)
        r.start()
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.pcm(1, 0, 4_800))
        r.videoBackend.events.add(r.output(2, 20_000))
        r.pump.pump(4)
        r.sink.head = 960
        r.videoBackend.events.add(r.output(3, 40_000))
        r.pump.pump(2)
        val idleFrom = r.clock.now
        r.sink.room = 0
        r.audioBackend.events.add(r.pcm(4, 100_000, 4_800))
        r.pump.pump(2)
        r.clock.now = idleFrom + 2_000_000_000 - 1
        r.pump.pump(2)
        assertEquals(listOf("v:release:2:7000000000"), releases(r), "the head has not moved: the 40 ms frame waits")
        assertTrue(r.log.none { it == "a:release:4:null" }, "a full, leading sink keeps its output")
        r.clock.now = idleFrom + 2_000_000_000
        r.pump.pump(2)
        assertEquals(listOf("audio output stalled; playing video only"), r.notices)
        assertEquals("v:release:3:${idleFrom + 2_000_000_000}", releases(r).last(), "the held frame shows at once on the display clock")
        assertTrue("a:release:4:null" in r.log, "a detached sink's refused PCM goes back to its decoder")
        assertEquals(19_200L, r.out!!.droppedBytes)
        r.videoBackend.events.add(r.output(5, 80_000))
        r.pump.pump(1)
        assertEquals(2, releases(r).size, "the next frame waits for its display deadline")
        r.clock.now += 40_000_000
        r.pump.pump(1)
        assertEquals("v:release:5:${idleFrom + 2_040_000_000}", releases(r).last())
        assertEquals(1, r.notices.size)
    }

    @Test fun `audio that has played out stops leading without a report and the video tail keeps its own time`() {
        val r = Rig(units = 8)
        r.start()
        r.audioBackend.events.add(PlaybackCodecEvent.Format(pcm))
        r.audioBackend.events.add(r.pcm(1, 0, 4_800, end = true))
        r.videoBackend.events.add(r.output(2, 20_000))
        r.pump.pump(4)
        r.sink.head = 4_800
        r.clock.now += 100_000_000
        r.videoBackend.events.add(r.output(3, 120_000))
        r.videoBackend.events.add(r.output(4, 160_000, size = 4, end = true))
        r.pump.pump(2)
        assertEquals("v:release:3:7100000000", releases(r).last())
        assertFalse(r.out!!.leading)
        assertFalse(r.pump.decoderOutputsEnded, "the last frame is still held for its deadline")
        r.clock.now += 40_000_000
        r.pump.pump(1)
        assertEquals("v:release:4:7140000000", releases(r).last())
        assertTrue(r.pump.decoderOutputsEnded)
        assertEquals(emptyList(), r.notices)
    }
}
