package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros

/**
 * The publish-side media contract this sample owns.
 *
 * An encoder hands back a buffer, a byte range inside it and a flag word, and
 * takes that buffer BACK as soon as it is released. Everything about how those
 * become MoQ objects is decided here, over ordinary values, so it can be
 * exercised on the host: the codec-owned buffer is represented exactly as
 * `MediaCodec.BufferInfo` describes it — a whole buffer plus an offset and a
 * size, never "the buffer" — and the bytes are COPIED before release.
 *
 * **Timebase.** [PublishTimeline] carries ONE origin for the whole broadcast,
 * DECLARED before any output is admitted. Rebasing each stream on its own
 * first frame would erase the real offset between audio and video start, and
 * taking the origin from whichever output is dequeued first would reject an
 * earlier audio capture that arrives after a later video output -- encoder
 * callbacks are per codec and carry no global ordering. Monotonicity is
 * therefore checked PER TRACK, never against one shared last-PTS maximum, and
 * a codec-configuration or empty end-of-stream buffer carries a timestamp that
 * means nothing on the media timeline and advances nothing.
 *
 * **Ordering.** This tranche assumes an encoder configured WITHOUT frame
 * reordering, so output presentation times are monotonic and a decode time
 * distinct from the presentation time does not arise. That is a stated
 * constraint on the encoder configuration, not a property of AVC: a reordering
 * encoder needs the DTS/PTS distinction carried explicitly, and is out of
 * scope here rather than silently assumed away.
 *
 * **Wall clock.** On a RAW/LOC track exactly one timestamp
 * reaches the peer, and it is the LOC Capture Timestamp: epoch wall-clock
 * microseconds (LOC-01 2.3.1.1). The publisher therefore supplies an EXPLICIT,
 * KNOWN epoch capture instant for every media object, and an object whose
 * capture instant is unknown or unrepresentable is REFUSED by name. The
 * sender's absent-capture fallback -- which would put a relative media time in
 * that field -- is never relied upon, and the presentation time the app
 * supplies locally is not claimed to survive transmission.
 */
internal object CaptureContract {
    /** `MediaCodec.BUFFER_FLAG_KEY_FRAME`. */
    const val FLAG_KEY_FRAME: Int = 1

    /** `MediaCodec.BUFFER_FLAG_CODEC_CONFIG`. */
    const val FLAG_CODEC_CONFIG: Int = 2

    /** `MediaCodec.BUFFER_FLAG_END_OF_STREAM`. */
    const val FLAG_END_OF_STREAM: Int = 4

    /** `MediaCodec.BUFFER_FLAG_PARTIAL_FRAME`. */
    const val FLAG_PARTIAL_FRAME: Int = 8
}

/**
 * Which codec produced an output. The decision differs by kind: an AAC access
 * unit is independently decodable on its own, so audio independence is not the
 * video key-frame signal, and group cadence is a publishing decision separate
 * from independence.
 */
internal enum class MediaKind { Video, Audio }

/**
 * One encoder output, as `dequeueOutputBuffer` plus `BufferInfo` describe it.
 * [buffer] is the codec's buffer; only `[offset, offset + size)` is this
 * output, and the range is VALIDATED before anything is copied.
 */
internal class EncodedOutput(
    val buffer: ByteArray,
    val offset: Int,
    val size: Int,
    val presentationTimeUs: Long,
    val flags: Int,
) {
    val isCodecConfig: Boolean get() = flags and CaptureContract.FLAG_CODEC_CONFIG != 0
    val isKeyFrame: Boolean get() = flags and CaptureContract.FLAG_KEY_FRAME != 0
    val isEndOfStream: Boolean get() = flags and CaptureContract.FLAG_END_OF_STREAM != 0
    val isPartial: Boolean get() = flags and CaptureContract.FLAG_PARTIAL_FRAME != 0
}

/**
 * What one encoder output becomes.
 *
 * [Config] is the codec configuration and is NEVER published as a frame.
 * [Frame] carries the exact object to write, and [Frame.endsStream] is true
 * when the codec set end-of-stream on that same nonempty buffer: the media and
 * the terminal are both obligations, and neither may be dropped for the other.
 * [EndOfStream] is the separate empty terminal. [Rejected] is a named refusal.
 */
internal sealed interface OutputDecision {
    data class Config(val bytes: ByteArray) : OutputDecision {
        override fun equals(other: Any?): Boolean = other is Config && bytes.contentEquals(other.bytes)

        override fun hashCode(): Int = bytes.contentHashCode()

        override fun toString(): String = "Config(" + bytes.joinToString("") { "%02x".format(it) } + ")"
    }

    data class Frame(val obj: SendObject, val endsStream: Boolean = false) : OutputDecision

    data object EndOfStream : OutputDecision

    data class Rejected(val reason: String) : OutputDecision
}

/**
 * The broadcast's media timeline: encoder presentation times made relative to
 * ONE origin, declared up front, with per-track monotonicity.
 *
 * A step backwards WITHIN a track is refused rather than wrapped into an
 * unsigned value. A later output on one track never constrains an earlier one
 * on another: that is ordinary A/V interleaving, not a fault.
 */
internal class PublishTimeline(private val origin: Long) {
    private val last = HashMap<MediaKind, Long>()

    /** The broadcast's declared origin on the encoder clock. */
    val originUs: Long
        get() = origin

    /**
     * The media time of one output, relative to the declared origin, checked
     * for monotonicity WITHIN its own track only.
     */
    fun relativeUs(kind: MediaKind, presentationTimeUs: Long): Long =
        requireNotNull(advanceOrNull(kind, presentationTimeUs)) {
            "this track's presentation time went backwards, or has no representable offset"
        }

    /**
     * The same, as a value: `null` when the step is backwards within this track
     * or the offset does not fit. Only an ACCEPTED output advances the track.
     */
    fun advanceOrNull(kind: MediaKind, presentationTimeUs: Long): Long? {
        val previous = last[kind]
        if (previous != null && presentationTimeUs < previous) return null
        if (presentationTimeUs < origin) return null
        val offset = presentationTimeUs - origin
        if (offset < 0) return null // the subtraction itself overflowed
        last[kind] = presentationTimeUs
        return offset
    }
}

/**
 * Turns one encoder output into a decision. The codec-owned bytes are copied
 * out of [EncodedOutput.buffer] here, because the caller releases that buffer
 * immediately afterwards.
 */
internal object EncodedOutputs {
    /**
     * [captureTime] is the object's epoch capture instant. Under the ruled RAW
     * policy it is REQUIRED for a media frame: `null` is a named refusal, never
     * a relative value sent in the epoch field.
     */
    fun decide(
        output: EncodedOutput,
        kind: MediaKind,
        timeline: PublishTimeline,
        captureTime: UnixMicros?,
    ): OutputDecision {
        // The range is validated BEFORE anything is copied and before the
        // timeline is touched: a rejected output must change nothing.
        if (output.offset < 0 || output.size < 0) {
            return OutputDecision.Rejected("an encoder output range is non-negative")
        }
        val end = output.offset.toLong() + output.size.toLong()
        if (end > output.buffer.size.toLong()) {
            return OutputDecision.Rejected("the output range runs past the codec buffer")
        }
        if (output.isPartial) {
            return OutputDecision.Rejected("a partial frame is not an access unit")
        }
        // The codec-owned bytes are copied here, because the caller releases
        // that buffer immediately afterwards.
        val bytes = output.buffer.copyOfRange(output.offset, output.offset + output.size)
        if (output.isCodecConfig) {
            // A configuration buffer's timestamp means nothing on the media
            // timeline, so it advances nothing.
            return OutputDecision.Config(bytes)
        }
        if (output.size == 0) {
            return if (output.isEndOfStream) {
                OutputDecision.EndOfStream
            } else {
                OutputDecision.Rejected("an empty output that is not the terminal carries nothing")
            }
        }
        // The ruled RAW policy: the LOC Capture Timestamp is an epoch instant,
        // so an unknown capture clock is refused, before the clock advances.
        val capture = captureTime
            ?: return OutputDecision.Rejected("this frame has no known epoch capture instant")
        val relative = timeline.advanceOrNull(kind, output.presentationTimeUs)
            ?: return OutputDecision.Rejected("this track's presentation time went backwards")
        // An AAC access unit is independently decodable on its own; only video
        // takes its independence from the key-frame flag. Group cadence is a
        // separate decision, so an audio unit does not open a group here.
        val sync = if (kind == MediaKind.Audio) true else output.isKeyFrame
        val startsGroup = kind == MediaKind.Video && output.isKeyFrame
        return OutputDecision.Frame(
            SendObject(
                payload = MediaBytes(bytes),
                isSync = sync,
                startsGroup = startsGroup,
                presentationTime = MediaMicros(relative.toULong()),
                captureTime = capture,
            ),
            endsStream = output.isEndOfStream,
        )
    }
}

/**
 * What abandoning a refused write requires of the next object.
 *
 * A video group's later frames depend on its sync frame and on each other, so
 * dropping one and continuing would publish a group the decoder cannot
 * assemble. Abandoning a non-sync video object therefore demands a restart at
 * the next sync frame. An AAC access unit is independently decodable, so audio
 * resumes.
 *
 * This is a LOCAL admission policy. It stops this sample from publishing
 * frames that depend on what was lost; it is not evidence about the group
 * state the service or the peer holds, and nothing here claims a native group
 * was reset.
 */
internal sealed interface Recovery {
    data object Resume : Recovery

    data object RestartAtSync : Recovery
}

/** Whether an object may be written now, or why it may not. */
internal sealed interface SequenceOutcome {
    data object Admitted : SequenceOutcome

    data class Deferred(val reason: String) : SequenceOutcome
}

/**
 * The publish sequence for one track: what may be written, what may follow an
 * abandoned write, and when the track may be ended.
 */
internal class PublishSequence(private val kind: MediaKind) {
    // One work item at a time, held by IDENTITY: this sample writes one object
    // and retries THAT object, so a second admission of the same item would be
    // a second write of one frame. Two separately produced frames with equal
    // bytes and timestamps are different work, which is why nothing here
    // compares by value.
    private var inFlight: SendObject? = null

    // The last settled item, so a retry that already succeeded is refused
    // exactly once. It is ONE slot: a completed broadcast must not retain its
    // whole history of payloads.
    private var settled: SendObject? = null
    private var needsSync = kind == MediaKind.Video

    /** The payload bytes this sequence still holds: bounded by construction. */
    val retainedPayloadBytes: Int
        get() = (inFlight?.payload?.size ?: 0) + (settled?.payload?.size ?: 0)

    fun admit(obj: SendObject): SequenceOutcome {
        if (inFlight != null) {
            return SequenceOutcome.Deferred(
                if (inFlight === obj) {
                    "this work item is already in flight; a refusal is retried through PendingWrite"
                } else {
                    "another work item is in flight; this sample writes one object at a time"
                },
            )
        }
        if (settled === obj) {
            return SequenceOutcome.Deferred("this work item was already published; a successful retry is spent")
        }
        if (needsSync && !obj.isSync) {
            return SequenceOutcome.Deferred("this track needs a sync frame before a frame that depends on one")
        }
        inFlight = obj
        if (obj.isSync) needsSync = false
        return SequenceOutcome.Admitted
    }

    fun onAccepted(obj: SendObject) {
        check(inFlight === obj) { "this work item was never admitted; settling it would record a write nobody made" }
        inFlight = null
        settled = obj
    }

    fun onAbandoned(obj: SendObject): Recovery {
        check(inFlight === obj) { "this work item was never admitted; abandoning it would rewrite unrelated state" }
        inFlight = null
        settled = null
        if (kind == MediaKind.Video) {
            // Whatever was lost, this group can no longer be assembled: the
            // frames that follow it depend on it or on its opener.
            needsSync = true
            return Recovery.RestartAtSync
        }
        return Recovery.Resume
    }

    /**
     * True only when no admitted object is still in flight: ending the track
     * while a final frame is pending would publish the terminal ahead of the
     * media it terminates.
     */
    fun mayEndTrack(): Boolean = inFlight == null
}

/**
 * A write that flow control refused, and the one place its retry state lives.
 *
 * `WouldBlock` transferred nothing, so the retry is the SAME value: rebuilding
 * it from a codec buffer that has since been reused would publish different
 * bytes. Once accepted, the pending write is spent — asking for it again is a
 * programming error, not a second write of the same object.
 */
internal class PendingWrite(private val obj: SendObject) {
    private var pending = true

    /** True while this write still has to be attempted. */
    val isPending: Boolean
        get() = pending

    fun next(): SendObject {
        check(pending) { "this write was already accepted; asking for it again would write it twice" }
        return obj
    }

    fun onAccepted() {
        check(pending) { "this write was already accepted" }
        pending = false
    }

    /** The payload this write will always carry, for diagnostics. */
    val payload: MediaBytes get() = obj.payload
}
