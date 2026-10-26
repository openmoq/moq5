package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.SendObject

/**
 * One encoder output's journey from the codec to a write, on the accepted
 * contracts rather than beside them.
 *
 * The session used to build a `SendObject` straight from a byte generator. That
 * skipped every decision this tranche established: the buffer's declared range
 * and its copy, the codec-configuration outcome, the key-frame/independence
 * rule, the shared broadcast timeline, the epoch capture instant, and the
 * admission/retry state. This seam runs them in order, so the session becomes a
 * loop over outcomes instead of a second implementation of them.
 *
 * It is Android-free: the encoder is an [EncodedOutputSource], and the capture
 * instant comes from an already-calibrated [PlatformCaptureClock]. A real
 * `MediaCodec` loop implements the same source; the synthetic generator is a
 * FIXTURE behind it, never a second product path.
 */
/**
 * One encoder output together with the instant it was CAPTURED, on the
 * elapsed-realtime base the clock is calibrated against.
 *
 * The timestamp belongs to the source, not to whatever loop drains it: a real
 * encoder emits when it emits, and its capture instant is a platform fact, not
 * a frame index times a nominal interval. Keeping the two together is what
 * stops a consumer from re-deriving one from the other.
 */
internal data class CapturedOutput(val output: EncodedOutput, val captureElapsedNanos: Long)

internal fun interface EncodedOutputSource {
    /** The next output and its capture instant, or `null` when there are none left. */
    fun next(): CapturedOutput?
}

/**
 * The frame source this sample ships until a real encoder is wired: a
 * deterministic byte generator behind the SAME seam a `MediaCodec` output loop
 * will implement. It is a FIXTURE: each output is ONE declared Annex-B NAL unit
 * -- `00 00 00 01`, an IDR (0x65) or non-IDR slice (0x41) header, then
 * recognisable non-zero bytes -- so the publish path, conversion included, can
 * be exercised. It is NOT encoded video and never a decodable picture, and it
 * is never a second way to publish.
 */
internal class SyntheticEncoderOutputs(
    private val baseElapsedNanos: Long,
    private val frames: Int,
    private val gop: Int,
    private val frameIntervalUs: Long = 33_333L,
    private val keyframeBytes: Int = DEFAULT_KEYFRAME_BYTES,
    private val deltaBytes: Int = 512,
) : EncodedOutputSource {
    init {
        require(keyframeBytes > HEADER_BYTES && deltaBytes > HEADER_BYTES) { "a synthetic output holds its start code, header and at least one byte" }
    }

    private var index = 0

    override fun next(): CapturedOutput? {
        if (index >= frames) return null
        val at = index++
        val keyframe = gop > 0 && at % gop == 0
        val size = if (keyframe) keyframeBytes else deltaBytes
        // A recognisable, deterministic payload. It is NOT encoded video: the
        // body never holds a zero byte, so no start code or escape can appear
        // inside the one NAL unit.
        val bytes =
            ByteArray(size) { i ->
                when (i) {
                    0, 1, 2 -> 0.toByte()
                    3 -> 1.toByte()
                    4 -> if (keyframe) IDR_HEADER else SLICE_HEADER
                    else -> ((at + i) % 255 + 1).toByte()
                }
            }
        // The fixture owns its own capture instants, deterministically, the
        // way a real encoder owns the platform timestamps it reports.
        return CapturedOutput(
            output =
                EncodedOutput(
                    buffer = bytes,
                    offset = 0,
                    size = size,
                    presentationTimeUs = at.toLong() * frameIntervalUs,
                    flags = if (keyframe) CaptureContract.FLAG_KEY_FRAME else 0,
                ),
            captureElapsedNanos = baseElapsedNanos + at.toLong() * frameIntervalUs * 1_000L,
        )
    }

    internal companion object {
        const val DEFAULT_KEYFRAME_BYTES = 4_096
        private const val HEADER_BYTES = 5
        private const val IDR_HEADER: Byte = 0x65
        private const val SLICE_HEADER: Byte = 0x41
    }
}

/** What the session should do with one encoder output. */
internal sealed interface PublishStep {
    /** Write this object; [endsStream] means the encoder ended on it. */
    data class Write(val obj: SendObject, val endsStream: Boolean) : PublishStep

    /** Codec configuration: it belongs to the track, never to a write. */
    data class Configure(val bytes: ByteArray) : PublishStep {
        override fun equals(other: Any?): Boolean = other is Configure && bytes.contentEquals(other.bytes)

        override fun hashCode(): Int = bytes.contentHashCode()
    }

    /** The encoder's terminal, with no media of its own. */
    data object EndTrack : PublishStep

    /** Nothing is written, and why. */
    data class Skipped(val reason: String) : PublishStep

    /**
     * This track cannot continue: a media payload the sample refuses to
     * convert, or a codec-configuration observation that does not match the
     * track's validated configuration. Nothing was admitted for it, and the
     * track ends here without a terminal.
     */
    data class Refused(val reason: String) : PublishStep
}

/**
 * The publish pipeline for one track, over the track's VALIDATED [format]:
 * its configuration is what the track was admitted with, and its authority is
 * what every later observation is compared with. A video frame is converted
 * to the avc1 payload within [limits] BEFORE it is admitted, so the object
 * admitted, written and settled is the converted one; an audio frame is
 * admitted as decided. [output] is where a converted payload is allocated.
 */
internal class PublishPipeline(
    private val kind: MediaKind,
    private val timeline: PublishTimeline,
    private val clock: PlatformCaptureClock,
    private val format: CompleteTrackFormat,
    private val limits: AvcPayloadLimits,
    private val output: AvcOutputAllocator = AvcOutputAllocator.HEAP,
) {
    init {
        require(format.kind == kind) { "a $kind pipeline was given a ${format.kind} format" }
    }

    private val sequence = PublishSequence(kind)

    /**
     * Runs one output through the capture clock, the output decision and the
     * admission policy. [captureNanos] is the platform timestamp that output
     * carried, on the clock's own base.
     */
    fun step(output: EncodedOutput, captureNanos: Long): PublishStep {
        // Configuration and the empty terminal carry no capture instant at all,
        // so they are decided before the clock is consulted.
        val capture =
            if (output.isCodecConfig || output.size == 0) {
                null
            } else {
                when (val instant = clock.instantOf(PlatformTimebase.ElapsedRealtime, captureNanos)) {
                    is CaptureInstant.At -> instant.epoch
                    is CaptureInstant.Unavailable -> return PublishStep.Skipped(instant.reason)
                }
            }
        return when (val decision = EncodedOutputs.decide(output, kind, timeline, capture)) {
            is OutputDecision.Config ->
                if (observedConfigurationMatches(decision.bytes)) {
                    PublishStep.Configure(decision.bytes)
                } else {
                    PublishStep.Refused("the $kind codec-configuration observation does not match its validated configuration")
                }
            OutputDecision.EndOfStream -> PublishStep.EndTrack
            is OutputDecision.Rejected -> PublishStep.Skipped(decision.reason)
            is OutputDecision.Frame -> {
                // Converted BEFORE admission: a refusal leaves nothing admitted.
                // The per-track timeline has already advanced for this frame; a
                // refusal ends the track, so nothing follows on it.
                val admitted =
                    when (val authority = format.authority) {
                        is FormatAuthority.Aac -> decision.obj
                        is FormatAuthority.Avc ->
                            when (val converted = AvcPayloads.convert(decision.obj.payload, authority.config.parameterSets, limits, this.output)) {
                                is AvcPayloadVerdict.Converted -> decision.obj.copy(payload = converted.payload)
                                is AvcPayloadVerdict.Malformed -> return refusedPayload(converted.defect.description)
                                is AvcPayloadVerdict.OutsideSubset -> return refusedPayload(converted.refusal.description)
                            }
                    }
                when (val admission = sequence.admit(admitted)) {
                    SequenceOutcome.Admitted -> PublishStep.Write(admitted, decision.endsStream)
                    is SequenceOutcome.Deferred -> PublishStep.Skipped(admission.reason)
                }
            }
        }
    }

    private fun refusedPayload(description: String) = PublishStep.Refused("the $kind payload was refused: $description")

    /**
     * Whether a codec-configuration observation is exactly the validated
     * configuration's own: for AVC every retained SPS then every retained
     * PPS, in their declared order, each after `00 00 00 01`; for AAC the
     * AudioSpecificConfig byte for byte. No trimming or normalization -- a
     * mismatch is an observation that differs, not proof of reconfiguration.
     */
    private fun observedConfigurationMatches(bytes: ByteArray): Boolean =
        when (val authority = format.authority) {
            is FormatAuthority.Aac -> format.config.initData?.bytes()?.contentEquals(bytes) == true
            is FormatAuthority.Avc -> {
                val sets = authority.config.parameterSets
                var at = 0
                fun next(set: ByteArray): Boolean {
                    if (at + START_CODE.size + set.size > bytes.size) return false
                    for (i in START_CODE.indices) if (bytes[at + i] != START_CODE[i]) return false
                    at += START_CODE.size
                    for (i in set.indices) if (bytes[at + i] != set[i]) return false
                    at += set.size
                    return true
                }
                (0 until sets.sequenceParameterSets).all { next(sets.sequenceParameterSet(it)) } &&
                    (0 until sets.pictureParameterSets).all { next(sets.pictureParameterSet(it)) } &&
                    at == bytes.size
            }
        }

    /** The write succeeded: settle it exactly once. */
    fun accepted(obj: SendObject) {
        sequence.onAccepted(obj)
    }

    /** The write was given up on: the recovery policy decides what may follow. */
    fun abandoned(obj: SendObject): Recovery = sequence.onAbandoned(obj)

    /** True when no admitted object is still in flight. */
    fun mayEndTrack(): Boolean = sequence.mayEndTrack()

    private companion object {
        val START_CODE = byteArrayOf(0, 0, 0, 1)
    }
}
