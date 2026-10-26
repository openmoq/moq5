package org.openmoq.moq5.samples.publisher

import android.media.MediaFormat
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/**
 * Encoder output formats to the typed holders: the extraction reads a
 * format's fields only once the observation is ADMITTED, while its
 * reservation is held, and hands the real validators exactly what the
 * format declared -- or a named refusal.
 *
 * The format is a map-backed [FormatFields] that mirrors the platform
 * getters' contract (an absent integer raises) and records every read in
 * order, so a row can prove what was read, when, and in which order. The
 * holders, validators, slots, session and coordinator are the real ones; the
 * codec utilities are the fixture's scripted answers. The actual
 * `MediaFormat` adapter compiles against the SDK and uses this same path;
 * these host rows cannot execute it.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class FormatExtractionTest {

    /** One output format: its declared values, every read in order, and an optional hold or raise inside one read. */
    private class Fields(private val values: Map<String, Any?>) : FormatFields {
        private val log = mutableListOf<String>()
        val entered = CountDownLatch(1)
        private val release = CountDownLatch(1)

        @Volatile var holdAt: String? = null

        @Volatile var raiseAt: String? = null

        @Volatile var raise: Throwable? = null

        @Volatile var duringHold: () -> Unit = {}

        fun reads(): List<String> = synchronized(log) { log.toList() }

        fun rescue() = release.countDown()

        private fun read(what: String) {
            synchronized(log) { log.add(what) }
            if (holdAt == what) {
                holdAt = null
                duringHold()
                entered.countDown()
                check(release.await(BOUND_SECONDS, TimeUnit.SECONDS)) { "the held read was never released" }
            }
            if (raiseAt == what) raise?.let { throw it }
        }

        override fun containsKey(key: String): Boolean {
            read("has:$key")
            return values.containsKey(key)
        }

        override fun getInteger(key: String): Int {
            read("int:$key")
            return checkNotNull(values[key] as Int?) { "no integer $key" }
        }

        override fun getString(key: String): String? {
            read("string:$key")
            return values[key] as String?
        }

        override fun getByteBuffer(key: String): ByteBuffer? {
            read("buffer:$key")
            return values[key] as ByteBuffer?
        }
    }

    /**
     * [payload] inside a larger, READ-ONLY buffer: junk before and after, the
     * position at its first byte, the limit after its last, and a mark set at
     * that position -- so any read that moves them, or reads past them, shows.
     */
    private class Borrowed(payload: ByteArray) {
        val buffer: ByteBuffer
        val position = 3
        val limit = 3 + payload.size

        init {
            val backing = ByteArray(payload.size + 6) { 0x5a }
            payload.copyInto(backing, 3)
            buffer = ByteBuffer.wrap(backing).asReadOnlyBuffer()
            buffer.limit(limit)
            buffer.position(position)
            buffer.mark()
            buffer.position(position)
        }

        fun assertUntouched(label: String) {
            assertEquals(position, buffer.position(), "$label: position untouched")
            assertEquals(limit, buffer.limit(), "$label: limit untouched")
            buffer.position(position + 1)
            buffer.reset()
            assertEquals(position, buffer.position(), "$label: mark untouched")
        }
    }

    private val sps = byteArrayOf(0, 0, 0, 1, 0x67, 0x42, 0x00, 0x1e, 0xab.toByte(), 0xcd.toByte())
    private val pps = byteArrayOf(0, 0, 0, 1, 0x68, 0xce.toByte(), 0x3c, 0x80.toByte())

    private fun videoValues(sps: Borrowed = Borrowed(this.sps), pps: Borrowed = Borrowed(this.pps)): MutableMap<String, Any?> =
        mutableMapOf(
            FormatKeys.MIME to FormatKeys.VIDEO_AVC,
            FormatKeys.WIDTH to 640,
            FormatKeys.HEIGHT to 480,
            FormatKeys.BIT_RATE to 1_000_000,
            FormatKeys.CSD_0 to sps.buffer,
            FormatKeys.CSD_1 to pps.buffer,
        )

    private fun audioValues(asc: Borrowed = Borrowed(FIXTURE_ASC)): MutableMap<String, Any?> =
        mutableMapOf(
            FormatKeys.MIME to FormatKeys.AUDIO_AAC,
            FormatKeys.SAMPLE_RATE to 48_000,
            FormatKeys.CHANNEL_COUNT to 2,
            FormatKeys.BIT_RATE to 96_000,
            FormatKeys.CSD_0 to asc.buffer,
        )

    private class Setup {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val holders = FormatHolders(rig.gate, VIDEO_CONFIG.name, AUDIO_CONFIG.name, FIXTURE_BOUNDS, ScriptedVideoUtility, ScriptedAudioUtility)
    }

    private fun causeChain(raised: Throwable?, expected: Throwable) = generateSequence(raised) { it.cause }.any { it === expected }

    /** The owner over the holders' sources; the row's own failure leaves as itself. */
    private suspend fun TestScope.owned(s: Setup, body: suspend (RunOwner) -> Unit): Result<BroadcastOutcome> {
        var outcome: Result<BroadcastOutcome>? = null
        val wrapper =
            runCatching {
                withRun({ s.rig.run(s.rig.formatPublication(s.holders.sources, BUDGET)) }) { owner ->
                    runCurrent()
                    body(owner)
                    runCurrent()
                    outcome = settled(owner)
                }
            }
        return outcome ?: throw (wrapper.exceptionOrNull() ?: AssertionError("the run produced no result"))
    }

    // -- the keys --------------------------------------------------------------------------------

    @Test
    fun `x1 the keys and MIME types are the platform's own`() {
        assertEquals(MediaFormat.KEY_MIME, FormatKeys.MIME)
        assertEquals(MediaFormat.KEY_WIDTH, FormatKeys.WIDTH)
        assertEquals(MediaFormat.KEY_HEIGHT, FormatKeys.HEIGHT)
        assertEquals(MediaFormat.KEY_BIT_RATE, FormatKeys.BIT_RATE)
        assertEquals(MediaFormat.KEY_SAMPLE_RATE, FormatKeys.SAMPLE_RATE)
        assertEquals(MediaFormat.KEY_CHANNEL_COUNT, FormatKeys.CHANNEL_COUNT)
        assertEquals(MediaFormat.MIMETYPE_VIDEO_AVC, FormatKeys.VIDEO_AVC)
        assertEquals(MediaFormat.MIMETYPE_AUDIO_AAC, FormatKeys.AUDIO_AAC)
        assertEquals("csd-0", FormatKeys.CSD_0)
        assertEquals("csd-1", FormatKeys.CSD_1)
    }

    // -- admission before any read ---------------------------------------------------------------

    @Test
    fun `x2 an ended or overlapping callback reads nothing, and an admitted one reads while its reservation is held`() = runTest {
        val s = setup()
        val first = Fields(videoValues())
        first.holdAt = "buffer:${FormatKeys.CSD_0}"
        val activeDuringRead = AtomicReference<Set<MediaKind>?>()
        first.duringHold = { activeDuringRead.set(s.rig.gate.formatInventory().active) }
        val worker = AtomicReference<Result<FormatOutcome>?>()
        val thread = HostWorker("video-format") { worker.set(runCatching { s.holders.video.onOutputFormat(first, VIDEO_CONFIG.bitrate) }) }
        val overlapping = Fields(videoValues())
        var overlapOutcome: FormatOutcome? = null
        // The held read is rescued on every exit before the bounded join, and
        // the join fails the row if the thread is still running after it --
        // attached to the row's own failure when there is one, never in its place.
        withSettlement(emptyList(), listOf(thread)) {
            try {
                thread.start()
                assertTrue(first.entered.await(BOUND_SECONDS, TimeUnit.SECONDS), "the admitted callback is reading")
                overlapOutcome = s.holders.video.onOutputFormat(overlapping, VIDEO_CONFIG.bitrate)
            } finally {
                first.rescue()
            }
        }
        assertTrue(thread.isSettled, "the owned thread has terminated")
        assertEquals(setOf(MediaKind.Video), activeDuringRead.get(), "the admitted callback read while its reservation was held")
        assertSame(FormatOutcome.Overlapping, overlapOutcome)
        assertEquals(emptyList(), overlapping.reads(), "the overlapping callback read no field and no codec data")
        assertIs<FormatOutcome.Resolved>(checkNotNull(worker.get()).getOrThrow())

        val ended = Setup()
        ended.rig.forward.onFault(GENERATION, CaptureFault.Codec(MediaKind.Audio, "the audio encoder failed"))
        val late = Fields(audioValues())
        assertSame(FormatOutcome.Ended, ended.holders.audio.onOutputFormat(late, AUDIO_CONFIG.bitrate))
        assertEquals(emptyList(), late.reads(), "an ended session's callback read nothing")
    }

    private fun setup() = Setup()

    // -- exact extraction ------------------------------------------------------------------------

    @Test
    fun `x3 a video format yields the exact configuration from its ordered codec data, leaving the borrowed buffers untouched`() = runTest {
        val s = setup()
        val spsBuffer = Borrowed(sps)
        val ppsBuffer = Borrowed(pps)
        val fields = Fields(videoValues(spsBuffer, ppsBuffer))
        val maximum = 5_000_000_000uL
        val resolved = assertIs<FormatOutcome.Resolved>(s.holders.video.onOutputFormat(fields, maximum))
        assertEquals(VIDEO_FORMAT_CONFIG.copy(bitrate = maximum), resolved.format.config, "the declared maximum, full width, and nothing from the target")
        spsBuffer.assertUntouched("csd-0")
        ppsBuffer.assertUntouched("csd-1")
        val reads = fields.reads()
        assertTrue(reads.indexOf("buffer:${FormatKeys.CSD_0}") < reads.indexOf("buffer:${FormatKeys.CSD_1}"), "csd-0 before csd-1: $reads")
        assertEquals("has:${FormatKeys.MIME}", reads.first(), "the MIME type is checked first")
        assertTrue("int:${FormatKeys.BIT_RATE}" in reads, "the diagnostic target is read, and not carried")
    }

    @Test
    fun `x4 an audio format yields the exact configuration from csd-0, leaving the borrowed buffer untouched`() = runTest {
        val s = setup()
        val ascBuffer = Borrowed(FIXTURE_ASC)
        val fields = Fields(audioValues(ascBuffer))
        val maximum = ULong.MAX_VALUE
        val resolved = assertIs<FormatOutcome.Resolved>(s.holders.audio.onOutputFormat(fields, maximum))
        assertEquals(AUDIO_FORMAT_CONFIG.copy(bitrate = maximum), resolved.format.config)
        ascBuffer.assertUntouched("csd-0")
    }

    // -- named refusals --------------------------------------------------------------------------

    private fun firstRefusal(kind: MediaKind, values: Map<String, Any?>, maximum: ULong?): Pair<TrackFormatRefusal, List<String>> {
        val s = setup()
        val fields = Fields(values)
        val outcome = if (kind == MediaKind.Video) s.holders.video.onOutputFormat(fields, maximum) else s.holders.audio.onOutputFormat(fields, maximum)
        val refused = assertIs<FormatOutcome.Refused>(outcome, "a first observation that cannot be used fails its slot by name")
        assertEquals(emptySet(), s.rig.gate.formatInventory().active)
        return refused.failure.refusal to fields.reads()
    }

    @Test
    fun `x5 a missing or mismatched MIME, a missing fact or codec data, and empty or oversized codec data are named refusals`() {
        val mismatched = firstRefusal(MediaKind.Video, videoValues().also { it[FormatKeys.MIME] = FormatKeys.AUDIO_AAC }, VIDEO_CONFIG.bitrate)
        assertEquals(TrackFormatRefusal.Mime(FormatKeys.VIDEO_AVC, FormatKeys.AUDIO_AAC), mismatched.first)
        assertEquals(listOf("has:${FormatKeys.MIME}", "string:${FormatKeys.MIME}"), mismatched.second, "nothing else was read once the MIME disagreed")
        assertEquals(TrackFormatRefusal.Mime(FormatKeys.AUDIO_AAC, null), firstRefusal(MediaKind.Audio, audioValues().also { it.remove(FormatKeys.MIME) }, AUDIO_CONFIG.bitrate).first)
        assertEquals(TrackFormatRefusal.Mime(FormatKeys.AUDIO_AAC, FormatKeys.VIDEO_AVC), firstRefusal(MediaKind.Audio, audioValues().also { it[FormatKeys.MIME] = FormatKeys.VIDEO_AVC }, AUDIO_CONFIG.bitrate).first)
        assertEquals(TrackFormatRefusal.MissingFact("width"), firstRefusal(MediaKind.Video, videoValues().also { it.remove(FormatKeys.WIDTH) }, VIDEO_CONFIG.bitrate).first)
        assertEquals(TrackFormatRefusal.MissingFact("sample rate"), firstRefusal(MediaKind.Audio, audioValues().also { it.remove(FormatKeys.SAMPLE_RATE) }, AUDIO_CONFIG.bitrate).first)
        assertEquals(TrackFormatRefusal.MissingFact("maximum bitrate"), firstRefusal(MediaKind.Video, videoValues(), null).first, "a target bitrate never stands in for the declared maximum")
        assertEquals(TrackFormatRefusal.MissingFact("codec-specific data csd-0"), firstRefusal(MediaKind.Video, videoValues().also { it.remove(FormatKeys.CSD_0) }, VIDEO_CONFIG.bitrate).first)
        assertEquals(TrackFormatRefusal.MissingFact("codec-specific data csd-1"), firstRefusal(MediaKind.Video, videoValues().also { it.remove(FormatKeys.CSD_1) }, VIDEO_CONFIG.bitrate).first)
        assertEquals(TrackFormatRefusal.MissingFact("codec-specific data csd-0"), firstRefusal(MediaKind.Audio, audioValues().also { it.remove(FormatKeys.CSD_0) }, AUDIO_CONFIG.bitrate).first)
        assertEquals(
            TrackFormatRefusal.Snapshot("configuration unit 0 is empty"),
            firstRefusal(MediaKind.Video, videoValues(sps = Borrowed(ByteArray(0))), VIDEO_CONFIG.bitrate).first,
        )
        assertEquals(
            TrackFormatRefusal.Snapshot("configuration unit 0 of 5000 bytes exceeds the limit of 4096"),
            firstRefusal(MediaKind.Audio, audioValues(asc = Borrowed(ByteArray(5_000))), AUDIO_CONFIG.bitrate).first,
        )
    }

    // -- later formats and unexpected failures ---------------------------------------------------

    @Test
    fun `x6 later equal and changed formats go through the holder authority`() = runTest {
        val s = setup()
        var equal: FormatOutcome? = null
        var changed: FormatOutcome? = null
        val outcome =
            owned(s) {
                s.holders.video.onOutputFormat(Fields(videoValues()), VIDEO_CONFIG.bitrate)
                s.holders.audio.onOutputFormat(Fields(audioValues()), AUDIO_CONFIG.bitrate)
                runCurrent()
                equal = s.holders.audio.onOutputFormat(Fields(audioValues()), AUDIO_CONFIG.bitrate)
                changed = s.holders.video.onOutputFormat(Fields(videoValues().also { it[FormatKeys.WIDTH] = 1280 }), VIDEO_CONFIG.bitrate)
            }
        assertSame(FormatOutcome.Equal, equal)
        assertEquals(CaptureFault.Format(MediaKind.Video, "the video output format changed after it was validated"), assertIs<FormatOutcome.Changed>(changed).fault)
        assertIs<BroadcastOutcome.Unfinished>(outcome.getOrThrow())
        assertEquals(listOf("add:video", "add:audio", "remove:v", "remove:a"), s.rig.tracks.labels())
    }

    @Test
    fun `x7 an extraction that raises keeps the original cause, releases once, and is the owner's failure`() = runTest {
        for (raised in listOf<Throwable>(IllegalStateException("the format's buffer could not be read"), AssertionError("the format raised an Error"), kotlinx.coroutines.CancellationException("cancelled inside the format read"))) {
            val s = setup()
            val fields = Fields(videoValues())
            fields.raiseAt = "buffer:${FormatKeys.CSD_1}"
            fields.raise = raised
            var callerSaw: Throwable? = null
            val outcome = owned(s) { callerSaw = runCatching { s.holders.video.onOutputFormat(fields, VIDEO_CONFIG.bitrate) }.exceptionOrNull() }
            val label = raised::class.simpleName
            assertSame(raised, callerSaw, "$label: the callback's caller gets the original")
            assertEquals(emptySet(), s.rig.gate.formatInventory().active, "$label: released once")
            val receipt = s.rig.session.retirement()
            assertEquals(listOf(raised), receipt.causes, "$label: the session owns it")
            assertEquals(CaptureTermination.Failed(CaptureFault.Unexpected(MediaKind.Video, raised)), receipt.termination)
            assertTrue(causeChain(outcome.exceptionOrNull(), raised), "$label: the owner's failure: $outcome")
            assertEquals(emptyList(), s.rig.tracks.labels())
            assertTrue(s.rig.completion.calls.isEmpty())
        }
    }

    private companion object {
        val BUDGET = 5.seconds
        const val BOUND_SECONDS = 10L
    }
}
