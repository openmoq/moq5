package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The publish-side media contract, pinned by bytes, offsets and times.
 *
 * The fixtures describe what `MediaCodec` hands back: a buffer the codec owns,
 * a byte range inside it, a microsecond presentation time and a flag word. The
 * rows fix what is easiest to get wrong and impossible to see later — reading
 * the buffer instead of its range, keeping a reference to bytes the codec is
 * about to reuse, publishing the codec configuration as a frame, dropping the
 * media on a nonempty end-of-stream buffer, and treating an AAC access unit's
 * independence as a video key-frame signal.
 *
 * The timeline is the BROADCAST's, shared by every track, so a real audio/video
 * start offset survives; per-stream rebasing would erase it.
 */
class CaptureContractRedTest {

    /** The codec's buffer: the frame sits at offset 4, with other bytes around it. */
    private val frameBytes = byteArrayOf(0x11, 0x22, 0x33, 0x44, 0x55)
    private val codecBuffer = byteArrayOf(0x7f, 0x7e, 0x7d, 0x7c) + frameBytes + byteArrayOf(0x00, 0x00)
    private val configBytes = byteArrayOf(0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x1f)

    private val originUs = 123_456_789L
    private val secondPtsUs = 123_490_122L // one 30fps frame later: +33_333 us

    /** The broadcast's timeline: one origin, declared before any admission. */
    private fun timeline(origin: Long = originUs) = PublishTimeline(origin)

    private val capture = UnixMicros(1_746_104_600_000_000uL)

    private fun output(
        buffer: ByteArray = codecBuffer,
        offset: Int = 4,
        size: Int = frameBytes.size,
        ptsUs: Long = originUs,
        flags: Int = CaptureContract.FLAG_KEY_FRAME,
    ) = EncodedOutput(buffer, offset, size, ptsUs, flags)

    private fun sendObject(sync: Boolean = false, presentationUs: Long = 33_333) =
        SendObject(
            payload = MediaBytes(frameBytes),
            isSync = sync,
            startsGroup = sync,
            presentationTime = MediaMicros(presentationUs.toULong()),
        )

    // ---- o: the encoder output ---------------------------------------------

    @Test
    fun o1_onlyTheBuffersDeclaredRangeBecomesThePayload() {
        val frame = assertIs<OutputDecision.Frame>(EncodedOutputs.decide(output(), MediaKind.Video, timeline(), capture))
        assertContentEquals(frameBytes, frame.obj.payload.bytes(), "the payload is buffer[offset, offset + size)")
        assertEquals(frameBytes.size, frame.obj.payload.size, "not the whole codec buffer")
        assertFalse(frame.endsStream, "no end-of-stream flag was set")
    }

    @Test
    fun o2_theBytesAreCopiedBeforeTheCodecReusesTheBuffer() {
        val buffer = codecBuffer.copyOf()
        val frame = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(buffer = buffer), MediaKind.Video, timeline(), capture),
        )
        // releaseOutputBuffer(): the codec owns these bytes again and overwrites them.
        buffer.fill(0x5a)
        assertContentEquals(frameBytes, frame.obj.payload.bytes(), "the object must not alias a codec-owned buffer")
    }

    @Test
    fun o3_theCodecConfigurationIsNeverAFrameAndIsCopiedToo() {
        val buffer = byteArrayOf(0x40, 0x41) + configBytes
        val decision =
            EncodedOutputs.decide(
                EncodedOutput(buffer, 2, configBytes.size, 0L, CaptureContract.FLAG_CODEC_CONFIG),
                MediaKind.Video,
                timeline(),
                capture,
            )
        val config = assertIs<OutputDecision.Config>(decision, "csd goes to the track's configuration, not to a write")
        assertContentEquals(configBytes, config.bytes, "a config buffer's offset is honoured like any other")
        buffer.fill(0x5a)
        assertContentEquals(configBytes, config.bytes, "and its bytes are copied out of the codec's buffer")
    }

    @Test
    fun o4_theKeyFrameFlagGovernsVideoSyncAndTheGroupBoundary() {
        val key = assertIs<OutputDecision.Frame>(EncodedOutputs.decide(output(), MediaKind.Video, timeline(), capture))
        assertTrue(key.obj.isSync)
        assertTrue(key.obj.startsGroup, "only a sync object may open a group")

        val delta = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = secondPtsUs, flags = 0), MediaKind.Video, timeline(), capture),
        )
        assertFalse(delta.obj.isSync)
        assertFalse(delta.obj.startsGroup, "a delta frame must not open a group")
    }

    @Test
    fun o5_anAacOutputIsIndependentWithoutAnyKeyFrameFlag() {
        val audio = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = secondPtsUs, flags = 0), MediaKind.Audio, timeline(), capture),
        )
        assertTrue(audio.obj.isSync, "an AAC access unit is independently decodable; flags zero is not a delta")
    }

    @Test
    fun o6_aPartialOutputIsRefusedRatherThanPublishedAsAWholeFrame() {
        val decision =
            EncodedOutputs.decide(
                output(flags = CaptureContract.FLAG_KEY_FRAME or CaptureContract.FLAG_PARTIAL_FRAME),
                MediaKind.Video,
                timeline(),
                capture,
            )
        assertIs<OutputDecision.Rejected>(decision, "a partial frame is not an access unit")
    }

    @Test
    fun o7_aNonemptyEndOfStreamBufferKeepsItsMediaAndItsTerminal() {
        val decision =
            EncodedOutputs.decide(
                output(ptsUs = secondPtsUs, flags = CaptureContract.FLAG_END_OF_STREAM),
                MediaKind.Video,
                timeline(),
                capture,
            )
        val frame = assertIs<OutputDecision.Frame>(decision, "Android may set end-of-stream on the last nonempty buffer")
        assertContentEquals(frameBytes, frame.obj.payload.bytes(), "the final media is not dropped for the terminal")
        assertTrue(frame.endsStream, "and the terminal is not dropped for the media")
    }

    @Test
    fun o8_anEmptyEndOfStreamBufferIsTheTerminalAlone() {
        val decision =
            EncodedOutputs.decide(
                EncodedOutput(ByteArray(0), 0, 0, secondPtsUs, CaptureContract.FLAG_END_OF_STREAM),
                MediaKind.Video,
                timeline(),
                capture,
            )
        assertEquals(OutputDecision.EndOfStream, decision)
    }

    @Test
    fun o9_anInvalidRangeIsRefusedBeforeAnythingIsCopied() {
        val cases =
            listOf(
                "negative offset" to output(offset = -1),
                "negative size" to output(size = -1),
                "past the end" to output(offset = 4, size = codecBuffer.size),
                "overflowing sum" to output(offset = Int.MAX_VALUE, size = 8),
            )
        for ((why, bad) in cases) {
            assertIs<OutputDecision.Rejected>(
                EncodedOutputs.decide(bad, MediaKind.Video, timeline(), capture),
                "an out-of-range output is refused by name: $why",
            )
        }
    }

    // ---- t: the timeline ----------------------------------------------------

    @Test
    fun t1_theOriginIsDeclaredBeforeAnyOutputIsAdmitted() {
        val declared = PublishTimeline(originUs)
        assertEquals(originUs, declared.originUs, "the broadcast declares its origin up front")
        assertEquals(0L, declared.relativeUs(MediaKind.Video, originUs), "the media timeline starts at zero")
        assertEquals(33_333L, declared.relativeUs(MediaKind.Video, secondPtsUs), "microseconds, unscaled")
    }

    @Test
    fun t2_theObjectCarriesTheRelativeTimeNotTheEncodersRawStamp() {
        val frame = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = secondPtsUs, flags = 0), MediaKind.Video, timeline(), capture),
        )
        assertEquals(MediaMicros(33_333uL), frame.obj.presentationTime)
        assertNotEquals(MediaMicros(secondPtsUs.toULong()), frame.obj.presentationTime, "the raw stamp is not a media time")
    }

    @Test
    fun t3_aBackwardsStepWithinOneTrackIsRefusedRatherThanWrapped() {
        val shared = timeline()
        assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = secondPtsUs, flags = 0), MediaKind.Video, shared, capture),
        )
        val decision =
            EncodedOutputs.decide(output(ptsUs = originUs + 1_000, flags = 0), MediaKind.Video, shared, capture)
        assertIs<OutputDecision.Rejected>(decision, "this track's own presentation time went backwards")
    }

    @Test
    fun t4_anEarlierAudioOutputAfterALaterVideoOneIsNotOutOfOrder() {
        // Encoder callbacks are per codec: nothing orders them across tracks.
        // A shared last-PTS maximum would reject this legitimate interleaving.
        val shared = timeline()
        val video = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = originUs + 100_000, flags = 0), MediaKind.Video, shared, capture),
        )
        val audio = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = originUs + 10_000, flags = 0), MediaKind.Audio, shared, capture),
        )
        assertEquals(MediaMicros(100_000uL), video.obj.presentationTime)
        assertEquals(MediaMicros(10_000uL), audio.obj.presentationTime, "the real start offset survives, and is admitted")
    }

    @Test
    fun t5_configEmptyAndRejectedOutputsAdvanceNothing() {
        val shared = timeline()
        EncodedOutputs.decide(
            EncodedOutput(configBytes, 0, configBytes.size, originUs + 999_000L, CaptureContract.FLAG_CODEC_CONFIG),
            MediaKind.Video,
            shared,
            capture,
        )
        EncodedOutputs.decide(output(ptsUs = originUs + 999_000L, offset = -1), MediaKind.Video, shared, capture)
        EncodedOutputs.decide(
            EncodedOutput(ByteArray(0), 0, 0, originUs + 999_000L, CaptureContract.FLAG_END_OF_STREAM),
            MediaKind.Video,
            shared,
            capture,
        )
        val frame = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = originUs + 33_333L, flags = 0), MediaKind.Video, shared, capture),
        )
        assertEquals(
            MediaMicros(33_333uL),
            frame.obj.presentationTime,
            "none of those timestamps means anything on the media timeline",
        )
    }

    @Test
    fun t6_aMissingCaptureInstantIsRefusedWithoutAdvancingTheClock() {
        // The ruled RAW policy: the LOC Capture Timestamp is an epoch instant,
        // so an unknown capture clock is a refusal -- never a relative value
        // sent in that field, and never a clock read at send time. On ONE
        // timeline, so a decision made AFTER advancing the clock would show.
        val shared = timeline()
        val first = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = originUs + 10_000L, flags = 0), MediaKind.Video, shared, capture),
        )
        assertEquals(MediaMicros(10_000uL), first.obj.presentationTime)
        assertEquals(capture, first.obj.captureTime, "emitted verbatim")

        assertIs<OutputDecision.Rejected>(
            EncodedOutputs.decide(output(ptsUs = originUs + 90_000L, flags = 0), MediaKind.Video, shared, null),
            "a missing capture clock is a named refusal",
        )

        val intermediate = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(output(ptsUs = originUs + 50_000L, flags = 0), MediaKind.Video, shared, capture),
        )
        assertEquals(
            MediaMicros(50_000uL),
            intermediate.obj.presentationTime,
            "the refused frame never advanced this track's clock",
        )
    }

    // ---- r: refusal, retry and completion ----------------------------------

    @Test
    fun r1_aRefusedWriteIsRetriedAsTheSameValueAndIsSpentOnceAccepted() {
        val obj = sendObject(sync = true, presentationUs = 0)
        val pending = PendingWrite(obj)
        assertTrue(pending.isPending)
        val first = pending.next()
        val second = pending.next()
        assertSame(obj, first, "nothing was transferred, so the retry is the same object")
        assertSame(first, second)
        pending.onAccepted()
        assertFalse(pending.isPending, "an accepted write is spent")
        assertFailsWith<IllegalStateException>("asking for it again would write it twice") { pending.next() }
    }

    @Test
    fun r2_aVideoTrackMustOpenWithASyncFrame() {
        val sequence = PublishSequence(MediaKind.Video)
        assertIs<SequenceOutcome.Deferred>(
            sequence.admit(sendObject(sync = false, presentationUs = 0)),
            "a delta before any sync frame cannot be decoded by anyone",
        )
        assertEquals(SequenceOutcome.Admitted, sequence.admit(sendObject(sync = true, presentationUs = 0)))
    }

    @Test
    fun r3_anAdmittedDeltaThatIsAbandonedRequiresARestartAtTheNextSyncFrame() {
        val sequence = PublishSequence(MediaKind.Video)
        val opener = sendObject(sync = true, presentationUs = 0)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(opener))
        sequence.onAccepted(opener)

        // The refused object goes through the real admission transition first.
        val delta = sendObject(presentationUs = 33_333)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(delta))
        assertEquals(Recovery.RestartAtSync, sequence.onAbandoned(delta), "its group can no longer be assembled")

        assertIs<SequenceOutcome.Deferred>(
            sequence.admit(sendObject(presentationUs = 66_666)),
            "a later delta of the broken group must not be published as if nothing was lost",
        )
        assertEquals(
            SequenceOutcome.Admitted,
            sequence.admit(sendObject(sync = true, presentationUs = 100_000)),
            "the next sync frame restarts publication",
        )
    }

    @Test
    fun r4_anAbandonedOpeningSyncFrameStillRequiresASyncFrameToResume() {
        val sequence = PublishSequence(MediaKind.Video)
        val opener = sendObject(sync = true, presentationUs = 0)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(opener))
        assertEquals(Recovery.RestartAtSync, sequence.onAbandoned(opener), "the group never opened")
        assertIs<SequenceOutcome.Deferred>(sequence.admit(sendObject(presentationUs = 33_333)))
    }

    @Test
    fun r5_anAdmittedAudioAccessUnitThatIsAbandonedResumes() {
        val sequence = PublishSequence(MediaKind.Audio)
        val first = sendObject(sync = true, presentationUs = 0)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(first))
        sequence.onAccepted(first)

        val lost = sendObject(sync = true, presentationUs = 23_219)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(lost))
        assertEquals(Recovery.Resume, sequence.onAbandoned(lost))
        assertEquals(
            SequenceOutcome.Admitted,
            sequence.admit(sendObject(sync = true, presentationUs = 46_438)),
            "AAC access units do not depend on the one that was lost",
        )
    }

    @Test
    fun r6_theTrackIsNotEndedUntilTheFinalFrameItselfIsAccepted() {
        // A local sample-state proof: it says nothing about a native RESET or
        // about the terminal's order on the wire.
        val sequence = PublishSequence(MediaKind.Video)
        val opener = sendObject(sync = true, presentationUs = 0)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(opener))
        sequence.onAccepted(opener)
        assertTrue(sequence.mayEndTrack(), "nothing is in flight yet")

        // The last encoder output is nonempty AND carries end-of-stream.
        val final = assertIs<OutputDecision.Frame>(
            EncodedOutputs.decide(
                output(ptsUs = secondPtsUs, flags = CaptureContract.FLAG_END_OF_STREAM),
                MediaKind.Video,
                timeline(),
                capture,
            ),
        )
        assertTrue(final.endsStream)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(final.obj))
        assertFalse(sequence.mayEndTrack(), "the terminal must not precede the media it terminates")

        // Flow control refuses it once; the retry is the same value.
        val pending = PendingWrite(final.obj)
        assertSame(final.obj, pending.next())
        assertSame(final.obj, pending.next())
        assertFalse(sequence.mayEndTrack(), "still pending across the refusal")

        pending.onAccepted()
        sequence.onAccepted(final.obj)
        assertTrue(sequence.mayEndTrack(), "and now, exactly once")
        assertIs<SequenceOutcome.Deferred>(
            sequence.admit(final.obj),
            "a retry that already succeeded must not write a duplicate",
        )
    }

    // ---- w: work-item identity and bounded state (1766 F1) ------------------

    @Test
    fun w1_completedWorkIsNotRetainedForTheLifeOfTheBroadcast() {
        // A live publisher settles thousands of frames. The sequence may keep
        // its CURRENT work and the one item a retry could still refer to; it
        // must not accumulate every accepted payload. The oracle is the state
        // the sequence itself reports, not garbage-collection timing.
        val sequence = PublishSequence(MediaKind.Video)
        val payload = ByteArray(1024) { 0x5a }
        repeat(128) { index ->
            val obj =
                SendObject(
                    payload = MediaBytes(payload),
                    isSync = index == 0,
                    startsGroup = index == 0,
                    presentationTime = MediaMicros((index.toLong() * 33_333L).toULong()),
                )
            assertEquals(SequenceOutcome.Admitted, sequence.admit(obj))
            sequence.onAccepted(obj)
        }
        assertTrue(sequence.mayEndTrack(), "nothing is in flight after the last acceptance")
        assertTrue(
            sequence.retainedPayloadBytes <= 2 * payload.size,
            "the sequence retains bounded state, not 128 payloads: " + sequence.retainedPayloadBytes + " bytes",
        )
    }

    @Test
    fun w2_theSameWorkItemCannotBeInFlightTwice() {
        val sequence = PublishSequence(MediaKind.Video)
        val opener = sendObject(sync = true, presentationUs = 0)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(opener))
        assertIs<SequenceOutcome.Deferred>(
            sequence.admit(opener),
            "one work item, one write in flight; a retry uses PendingWrite, not a second admission",
        )
    }

    @Test
    fun w3_settlementIsRefusedForWorkThatWasNeverAdmitted() {
        val sequence = PublishSequence(MediaKind.Video)
        val stranger = sendObject(sync = true, presentationUs = 0)
        assertFailsWith<IllegalStateException>("accepting work nobody admitted would settle a phantom write") {
            sequence.onAccepted(stranger)
        }
        assertFailsWith<IllegalStateException>("and so would abandoning it") {
            sequence.onAbandoned(stranger)
        }
    }

    @Test
    fun w4_twoEqualValuedFramesAreDistinctWorkUnlessItIsTheSameItem() {
        val sequence = PublishSequence(MediaKind.Video)
        val first = sendObject(sync = true, presentationUs = 0)
        assertEquals(SequenceOutcome.Admitted, sequence.admit(first))
        sequence.onAccepted(first)

        // A separately produced frame that happens to carry equal bytes and an
        // equal timestamp is a DIFFERENT publisher work item.
        val equalButDistinct = sendObject(sync = true, presentationUs = 0)
        assertEquals(first, equalButDistinct, "the values really are equal")
        assertEquals(
            SequenceOutcome.Admitted,
            sequence.admit(equalButDistinct),
            "equal value is not the same work item",
        )
        sequence.onAccepted(equalButDistinct)
        assertIs<SequenceOutcome.Deferred>(
            sequence.admit(equalButDistinct),
            "but the SAME item, once accepted, is spent exactly once",
        )
    }

    // ---- n: what these oracles reject --------------------------------------
    //
    // These rows prove the assertions discriminate. They do NOT prove a live
    // production mutation was caught: the decisions are still stubs.

    @Test
    fun n1_theOracleRefusesTheWholeBufferAndAWrongOffset() {
        assertNotEquals(codecBuffer.size, frameBytes.size)
        assertFalse(codecBuffer.copyOfRange(3, 3 + frameBytes.size).contentEquals(frameBytes), "an off-by-one offset fails o1")
        assertFalse(codecBuffer.contentEquals(frameBytes))
    }

    @Test
    fun n2_theOracleRefusesAnAliasedBuffer() {
        val buffer = codecBuffer.copyOf()
        val aliased = buffer.copyOfRange(4, 4 + frameBytes.size)
        val alias = MediaBytes(aliased)
        aliased.fill(0x5a)
        assertContentEquals(frameBytes, alias.bytes(), "MediaBytes copies, so o2 fails only on a genuine alias")
        assertFalse(aliased.contentEquals(frameBytes), "the mutation o2 simulates really does change the bytes")
    }

    @Test
    fun n3_theT2ComparisonRejectsAWrongTimeMutant() {
        val raw = MediaMicros(secondPtsUs.toULong())
        val milliseconds = MediaMicros((secondPtsUs / 1_000).toULong())
        assertFailsWith<AssertionError>("t2 must refuse the encoder's own stamp") {
            assertEquals(MediaMicros(33_333uL), raw)
        }
        assertFailsWith<AssertionError>("t2 must refuse a millisecond scale") {
            assertEquals(MediaMicros(33_333uL), milliseconds)
        }
    }
}
