package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.SampleEntry

/**
 * The validated formats at the actual publish boundary: the production
 * format wait, owner, coordinator and drains ([Rig]), with only the sender
 * scripted. What is asserted is what reaches [TrackOperations] -- every Add,
 * Write and End, whole SendObjects with value bytes -- and the broadcast's
 * own outcome, never a helper verdict.
 *
 * The video format is validated through the real [TrackFormats] with TWO
 * sequence and TWO picture parameter sets, so a frame carrying the second
 * of each shows whether the retained authority, not only its first entry,
 * reaches the lane. Every frame is a declared Annex-B literal and every
 * expected payload a declared four-byte-length literal; the IDR and slice
 * bytes are framing vectors, not a valid coded picture.
 *
 * `c` rows are controls that hold today and must keep holding. `r` rows
 * are the integration obligations.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class PipelineIntegrationRedTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private fun cat(vararg parts: ByteArray): ByteArray = parts.fold(ByteArray(0)) { a, b -> a + b }

    private val sc4 = hex("00000001")
    private val sc3 = hex("000001")
    private val spsA = hex("6742001eabcd")
    private val spsB = hex("6742001fabcd")
    private val ppsA = hex("68ce3c80")
    private val ppsB = hex("68ee3c80")
    private val idr = hex("65888421")
    private val slice = hex("419a02")

    /** A key frame carrying the SECOND retained SPS and PPS, then an IDR and, after a three-byte prefix, a slice. */
    private val frame = cat(sc4, spsB, sc4, ppsB, sc4, idr, sc3, slice)

    /** Its avc1 payload: both sets stripped, each retained unit after its big-endian length. */
    private val converted = hex("0000000465888421" + "00000003419a02")

    /** A key frame whose parameter set, AFTER its slice, is in neither retained inventory. */
    private val lateUnknown = cat(sc4, idr, sc4, hex("6742001dabcd"))

    /** The video format's own configuration observation: every retained set, SPS then PPS, in order, each after 00 00 00 01. */
    private val videoConfig = cat(sc4, spsA, sc4, spsB, sc4, ppsA, sc4, ppsB)

    /** The same length, its last PPS's last byte changed. */
    private val videoConfigChanged = cat(sc4, spsA, sc4, spsB, sc4, ppsA, sc4, hex("68ee3c81"))

    /** A key frame whose unit AFTER its slice has forbidden_zero_bit set. */
    private val lateMalformed = cat(sc4, idr, sc4, hex("e5888421"))

    /** Access unit delimiter and SEI only: no coded slice. */
    private val noSlice = cat(sc4, hex("09f0"), sc4, hex("060501ab80"))

    /**
     * A frame the handoff admits at a 32-byte cap whose avc1 payload does not
     * fit it: an IDR, then four slices after three-byte prefixes, each of
     * which grows by one byte into a four-byte length.
     */
    private val overBound = cat(sc4, idr, sc3, slice, sc3, slice, sc3, slice, sc3, slice)
    private val overBoundConverted = 36

    /** AAC LC 44.1 kHz stereo: the same length as the fixture's 48 kHz configuration, different bytes. */
    private val audioConfigChanged = hex("1210")

    private object TwoSetUtility : AvcRecordUtility {
        override fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): UtilityOutcome<MediaBytes> =
            UtilityOutcome.Done(MediaBytes(SCRIPTED_VIDEO_RECORD))

        override fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes): UtilityOutcome<String> =
            UtilityOutcome.Done(VIDEO_CONFIG.codec.bytes().decodeToString())
    }

    private fun twoSetVideo(): CompleteTrackFormat {
        val observed =
            VideoFormatObservation(
                640,
                480,
                null,
                VIDEO_CONFIG.bitrate,
                listOf(ByteBuffer.wrap(cat(sc4, spsA, sc4, spsB)), ByteBuffer.wrap(cat(sc4, ppsA, sc4, ppsB))),
            )
        val verdict = TrackFormats.video(VIDEO_CONFIG.name, observed, ConfigBounds(4, 4096, 8192), TwoSetUtility)
        return assertIs<TrackFormatVerdict.Complete>(verdict, "setup: the two-set video format validates").format
    }

    private fun known(): FormatSources = FormatSources(CompletableDeferred(twoSetVideo()), CompletableDeferred(validatedAudio()))

    /** A media output with explicit [flags], its capture instant observed first, as the accepted [deliver] does for a key frame. */
    private fun deliverMedia(lane: Lane, index: Int, payload: ByteArray, pts: Long, flags: Int) {
        lane.buffers.put(index, ByteBuffer.wrap(payload))
        assertEquals(ObserveOutcome.Recorded, lane.ledger.observe(GENERATION, lane.kind, CaptureObservation(pts, BASE_NANOS + pts * 1_000L)))
        assertEquals(HandoffOutcome.Queued, lane.handoff.onOutput(GENERATION, CodecOutput(index, 0, payload.size, pts, flags)))
    }

    /** A codec-configuration output: no capture instant, never media. */
    private fun deliverConfig(lane: Lane, index: Int, bytes: ByteArray) {
        lane.buffers.put(index, ByteBuffer.wrap(bytes))
        assertEquals(HandoffOutcome.Queued, lane.handoff.onOutput(GENERATION, CodecOutput(index, 0, bytes.size, 0L, CaptureContract.FLAG_CODEC_CONFIG)))
    }

    private val key = CaptureContract.FLAG_KEY_FRAME
    private val keyEnd = CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_END_OF_STREAM

    /** Runs one broadcast over already-known formats; [feed] delivers outputs; returns the settled outcome. */
    private suspend fun TestScope.broadcast(rig: Rig, feed: suspend () -> Unit): BroadcastOutcome {
        val result =
            withRun({ rig.run(rig.formatPublication(known(), BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                feed()
                runCurrent()
                settled(owner)
            }
        return result.getOrThrow()
    }

    private fun rig() = Rig(ScriptedTracks().answers(V).answers(A))

    /**
     * A run that must refuse: after [feed], the sender has seen no write and
     * no end -- judged before settlement, so a write of forbidden bytes fails
     * here by inventory -- and the settled broadcast's whole inventory is the
     * two adds and the owner's retirement of both handles, nothing else.
     */
    private suspend fun TestScope.refusing(rig: Rig, feed: suspend () -> Unit): BroadcastOutcome {
        val result =
            withRun({ rig.run(rig.formatPublication(known(), BUDGET)) }) { owner ->
                runCurrent()
                stillLive(owner)
                feed()
                runCurrent()
                assertTrue(rig.tracks.all().none { it is Effect.Write || it is Effect.End }, "no write and no end: ${rig.tracks.labels()}")
                settled(owner)
            }
        expectEffects(retired(), rig.tracks.all())
        assertTrue(rig.completion.calls.isEmpty(), "no completion: ${rig.completion.calls}")
        return result.getOrThrow()
    }

    /** The two adds, then the owner's retirement of both handles: a refused broadcast's whole inventory. */
    private fun retired() = adds() + listOf(Effect.Remove(V), Effect.Remove(A))

    private fun adds() = listOf<Effect>(Effect.Add(VIDEO_FORMAT_CONFIG), Effect.Add(AUDIO_FORMAT_CONFIG))

    private fun refusedPayload(description: String) = "the Video payload was refused: $description"

    private fun observationMismatch(kind: MediaKind) =
        "the $kind codec-configuration observation does not match its validated configuration"

    // ---- c: controls ------------------------------------------------------------

    @Test
    fun `c0 the vectors have their declared sizes`() {
        assertEquals(32, frame.size, "4+6 + 4+4 + 4+4 + 3+3")
        assertEquals(15, converted.size, "4+4 + 4+3")
        assertEquals(18, lateUnknown.size, "4+4 + 4+6")
        assertEquals(videoConfig.size, videoConfigChanged.size, "the changed observation has the equal control's length")
        assertEquals(32, overBound.size, "4+4 + 4 x (3+3)")
        assertEquals(overBoundConverted, 4 + 4 + 4 * (4 + 3), "each three-byte prefix grows by one")
    }

    @Test
    fun `c1 audio media is written unchanged, and an empty terminal ends a track with no write`() = runTest {
        val rig = rig()
        val audioBytes = byteArrayOf(0x55, 0x66)
        val outcome =
            broadcast(rig) {
                deliver(rig.audio, 0, audioBytes, 21_333L)
                runCurrent()
                deliverEnd(rig.video, 0)
                runCurrent()
                deliverEnd(rig.audio, 1)
            }
        assertIs<BroadcastOutcome.Published>(outcome)
        expectEffects(
            adds() + listOf(Effect.Write(A, sendObject(audioBytes, 21_333L, startsGroup = false), WRITE_TIMEOUT), Effect.End(V, WRITE_TIMEOUT), Effect.End(A, WRITE_TIMEOUT)),
            rig.tracks.all(),
        )
    }

    @Test
    fun `c2 configuration observations equal to the validated formats have no effect`() = runTest {
        val rig = rig()
        val outcome =
            broadcast(rig) {
                deliverConfig(rig.video, 0, videoConfig)
                deliverConfig(rig.audio, 0, FIXTURE_ASC.copyOf())
                runCurrent()
                deliverEnd(rig.video, 1)
                runCurrent()
                deliverEnd(rig.audio, 1)
            }
        assertIs<BroadcastOutcome.Published>(outcome)
        expectEffects(adds() + listOf(Effect.End(V, WRITE_TIMEOUT), Effect.End(A, WRITE_TIMEOUT)), rig.tracks.all())
    }

    // ---- r: the integration obligations ---------------------------------------------

    @Test
    fun `r1 a video frame reaches the sender converted, its non-first parameter sets stripped`() = runTest {
        val rig = rig()
        val outcome =
            broadcast(rig) {
                deliverMedia(rig.video, 0, frame, 33_333L, key)
                runCurrent()
                deliverEnd(rig.video, 1)
                runCurrent()
                deliverEnd(rig.audio, 0)
            }
        assertIs<BroadcastOutcome.Published>(outcome)
        expectEffects(
            adds() + listOf(Effect.Write(V, sendObject(converted, 33_333L, startsGroup = true), WRITE_TIMEOUT), Effect.End(V, WRITE_TIMEOUT), Effect.End(A, WRITE_TIMEOUT)),
            rig.tracks.all(),
        )
    }

    @Test
    fun `r2 nonempty terminal media is converted, written and settled before endTrack`() = runTest {
        val rig = rig()
        val outcome =
            broadcast(rig) {
                deliverMedia(rig.video, 0, frame, 33_333L, keyEnd)
                runCurrent()
                deliverEnd(rig.audio, 0)
            }
        assertIs<BroadcastOutcome.Published>(outcome)
        expectEffects(
            adds() + listOf(Effect.Write(V, sendObject(converted, 33_333L, startsGroup = true), WRITE_TIMEOUT), Effect.End(V, WRITE_TIMEOUT), Effect.End(A, WRITE_TIMEOUT)),
            rig.tracks.all(),
        )
    }

    @Test
    fun `r3 a late unknown parameter set refuses the whole frame - no write, no end, the remainder discarded`() = runTest {
        val rig = rig()
        val outcome =
            refusing(rig) {
                deliverMedia(rig.video, 0, lateUnknown, 33_333L, key)
                deliverMedia(rig.video, 1, frame, 66_666L, key)
                deliverEnd(rig.video, 2)
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(refusedPayload("at 8: a type 7 parameter set that is not in the resolved configuration"), unfinished.reason)
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 2)), rig.forward.discards(), "the frame and terminal behind the refusal")
        assertEquals(1, rig.session.stops)
        assertTrue(rig.owner.isDecided)
    }

    @Test
    fun `r4 a refused terminal payload manufactures no endTrack`() = runTest {
        val rig = rig()
        val outcome = refusing(rig) { deliverMedia(rig.video, 0, lateUnknown, 33_333L, keyEnd) }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(refusedPayload("at 8: a type 7 parameter set that is not in the resolved configuration"), unfinished.reason)
        assertEquals(1, rig.session.stops)
    }

    @Test
    fun `r5 a same-length AVC configuration observation with a different PPS stops the track before its media`() = runTest {
        val rig = rig()
        val outcome =
            refusing(rig) {
                deliverConfig(rig.video, 0, videoConfigChanged)
                deliverMedia(rig.video, 1, frame, 33_333L, key)
                deliverEnd(rig.video, 2)
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(observationMismatch(MediaKind.Video), unfinished.reason)
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 2)), rig.forward.discards())
    }

    @Test
    fun `r6 a same-length AAC configuration observation with different bytes stops the track before its media`() = runTest {
        val rig = rig()
        assertEquals(FIXTURE_ASC.size, audioConfigChanged.size, "setup: the same length")
        val outcome =
            refusing(rig) {
                deliverConfig(rig.audio, 0, audioConfigChanged)
                deliver(rig.audio, 1, byteArrayOf(0x55, 0x66), 21_333L)
                deliverEnd(rig.audio, 2)
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(outcome)
        assertEquals(observationMismatch(MediaKind.Audio), unfinished.reason)
        assertEquals(listOf(Discard(GENERATION, MediaKind.Audio, 2)), rig.forward.discards())
    }

    @Test
    fun `r8 a malformed unit after the slice refuses the whole frame`() = runTest {
        val rig = rig()
        val outcome = refusing(rig) { deliverMedia(rig.video, 0, lateMalformed, 33_333L, key) }
        assertEquals(refusedPayload("at 8: forbidden_zero_bit is 1"), assertIs<BroadcastOutcome.Unfinished>(outcome).reason)
        assertEquals(emptyList(), rig.forward.discards(), "nothing remained behind it")
        assertEquals(1, rig.session.stops)
    }

    @Test
    fun `r9 a frame with no coded slice is refused, never written as a control-only frame`() = runTest {
        val rig = rig()
        val outcome = refusing(rig) { deliverMedia(rig.video, 0, noSlice, 33_333L, key) }
        assertEquals(refusedPayload("the frame retains no coded slice (type 1 or 5)"), assertIs<BroadcastOutcome.Unfinished>(outcome).reason)
        assertEquals(emptyList(), rig.forward.discards())
    }

    @Test
    fun `r10 the live lane bounds the converted payload by the handoff's own output cap`() = runTest {
        // The handoff admits a 32-byte output at a 32-byte cap; its avc1 payload needs 36.
        val rig = Rig(ScriptedTracks().answers(V).answers(A), maxOutputBytes = overBound.size)
        val outcome = refusing(rig) { deliverMedia(rig.video, 0, overBound, 33_333L, key) }
        assertEquals(
            refusedPayload("the converted payload needs $overBoundConverted bytes; the limit is ${overBound.size}"),
            assertIs<BroadcastOutcome.Unfinished>(outcome).reason,
        )
    }

    @Test
    fun `r7 a suspended write settles before a queued mismatch is consumed, and nothing follows it`() = runTest {
        val rig = rig()
        val held = rig.tracks.holdNextWrite(V)
        // The actual prepared lanes, observed through a forwarding publication:
        // the lanes and the drain are the production ones.
        val live = rig.formatPublication(known(), BUDGET)
        var lanes: Preparation.Ready? = null
        val observed = Publication.Live(live.ownership) { live.prepare().also { if (it is Preparation.Ready) lanes = it } }
        // The ordered ledger of what actually happened on the video track:
        // each dequeue, with whether an admission was outstanding when it was
        // made, and each write entering and returning.
        val ledger = java.util.Collections.synchronizedList(mutableListOf<String>())
        var admittedAtDequeue: Boolean? = null
        rig.videoEntry.beforeNext = { admittedAtDequeue = lanes?.video?.pipeline?.mayEndTrack()?.not() }
        rig.videoEntry.onNext = { item -> if (item != null) ledger += "dequeue ${describe(item)} admitted=$admittedAtDequeue" }
        rig.tracks.writeObserver = { if (it.startsWith("write v")) ledger += it }
        val result =
            withRun({ rig.run(observed) }) { owner ->
                runCurrent()
                stillLive(owner)
                deliverMedia(rig.video, 0, frame, 33_333L, key)
                runCurrent()
                withTimeoutOrNull(BOUNDED) { held.entered.await() } ?: throw AssertionError("the first write was never entered")
                // Queued BEHIND the suspended write: a mismatch, more media, the terminal.
                deliverConfig(rig.video, 1, videoConfigChanged)
                deliverMedia(rig.video, 2, frame, 66_666L, key)
                deliverEnd(rig.video, 3)
                runCurrent()
                stillLive(owner)
                assertFalse(owner.reached, "nothing settles while the write is suspended")
                assertEquals(listOf("dequeue media@33333 admitted=false", "write v entered"), ledger.toList(), "no later dequeue while the write is held")
                assertEquals(false, lanes?.video?.pipeline?.mayEndTrack(), "the converted frame is admitted while its write is held")
                expectEffects(adds() + Effect.Write(V, sendObject(converted, 33_333L, startsGroup = true), WRITE_TIMEOUT), rig.tracks.all())
                assertEquals(emptyList(), rig.forward.discards(), "nothing discarded while the write is suspended")
                held.release.complete(Unit)
                runCurrent()
                settled(owner)
            }
        assertEquals(
            listOf("dequeue media@33333 admitted=false", "write v entered", "write v returned", "dequeue config admitted=false"),
            ledger.toList(),
            "the write settled BEFORE the mismatch was dequeued, and nothing was dequeued after it",
        )
        assertEquals(true, lanes?.video?.pipeline?.mayEndTrack(), "no admission is left outstanding")
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(result.getOrThrow())
        assertEquals(observationMismatch(MediaKind.Video), unfinished.reason)
        expectEffects(
            adds() + listOf(Effect.Write(V, sendObject(converted, 33_333L, startsGroup = true), WRITE_TIMEOUT), Effect.Remove(V), Effect.Remove(A)),
            rig.tracks.all(),
        )
        assertTrue(rig.completion.calls.isEmpty())
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 2)), rig.forward.discards(), "the media and terminal behind the mismatch")
        assertEquals(1, rig.session.stops)
        assertTrue(rig.owner.isDecided)
    }

    /**
     * The existing capture-fault route, observed while a write is suspended --
     * the route a later changed format is proposed to take. A track fault
     * delivered independently of the drain (here the handoff's own codec
     * error, a stale generation first) ends the session WITHOUT the held write
     * being released: the drain is cancelled, the held write settles by the
     * existing contract, and nothing follows it.
     */
    @Test
    fun `r11 an independent track fault while a write is held ends the broadcast before the write returns`() = runTest {
        val rig = rig()
        val held = rig.tracks.holdNextWrite(V)
        val live = rig.formatPublication(known(), BUDGET)
        var lanes: Preparation.Ready? = null
        val observed = Publication.Live(live.ownership) { live.prepare().also { if (it is Preparation.Ready) lanes = it } }
        val result =
            withRun({ rig.run(observed) }) { owner ->
                runCurrent()
                stillLive(owner)
                deliverMedia(rig.video, 0, frame, 33_333L, key)
                runCurrent()
                withTimeoutOrNull(BOUNDED) { held.entered.await() } ?: throw AssertionError("the first write was never entered")
                deliverMedia(rig.video, 1, frame, 66_666L, key)
                runCurrent()
                // A stale generation is inert.
                rig.video.handoff.onCodecError(GENERATION + 1, "a fault from another generation")
                runCurrent()
                stillLive(owner)
                assertEquals(null, rig.session.termination, "the stale fault decided nothing")
                // The current generation's fault, delivered while the write is still held.
                rig.video.handoff.onCodecError(GENERATION, "the video encoder failed")
                runCurrent()
                assertTrue(owner.reached, "settled without the held write being released")
                assertFalse(held.release.isCompleted)
                settled(owner)
            }
        val unfinished = assertIs<BroadcastOutcome.Unfinished>(result.getOrThrow())
        // The session's own terminal is the fault. The outcome's reason is the
        // coordinator's existing arbitration: the first drain to settle -- the
        // idle audio track, whose handoff the failed close ended -- names it.
        assertEquals(CaptureTermination.Failed(CaptureFault.Codec(MediaKind.Video, "the video encoder failed")), rig.session.termination)
        assertEquals("the Audio track ended without a terminal", unfinished.reason)
        assertEquals(true, lanes?.video?.pipeline?.mayEndTrack(), "the held write's admission was settled")
        expectEffects(
            adds() + listOf(Effect.Write(V, sendObject(converted, 33_333L, startsGroup = true), WRITE_TIMEOUT), Effect.Remove(V), Effect.Remove(A)),
            rig.tracks.all(),
        )
        assertTrue(rig.completion.calls.isEmpty(), "no completion")
        assertEquals(listOf(Discard(GENERATION, MediaKind.Video, 1)), rig.forward.discards(), "the queued frame behind the held write")
        assertEquals(1, rig.session.stops)
    }

    private fun describe(item: CapturedOutput): String =
        when {
            item.output.isCodecConfig -> "config"
            item.output.size == 0 -> "terminal"
            else -> "media@${item.output.presentationTimeUs}"
        }

    private companion object {
        val BUDGET = 5.seconds
        val V = HostTrack("v")
        val A = HostTrack("a")
    }
}
