package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.MediaBytes

/**
 * Explicit, positive bounds on converting one frame: how many NAL units it
 * may hold (stripped parameter sets included), and how many bytes the
 * converted payload may take. The input cap does not bound the output -- a
 * three-byte start code becomes a four-byte length -- so the output is
 * bounded on its own.
 */
internal class AvcPayloadLimits(val maxNalUnits: Int, val maxOutputBytes: Int) {
    init {
        require(maxNalUnits > 0) { "the NAL unit limit is positive, not $maxNalUnits" }
        require(maxOutputBytes > 0) { "the output byte limit is positive, not $maxOutputBytes" }
    }
}

/**
 * An owned, immutable set of AVC parameter sets: at most
 * [AvcConfiguration.MAX_PARAMETER_SETS] of each type, each 1 to
 * [AvcConfiguration.MAX_PARAMETER_SET_BYTES] bytes, enforced here before
 * anything is copied. Accessors return copies and [matches] compares in place,
 * so no caller or accessor alias can change a later comparison. Positions are
 * the declared order.
 *
 * The type does not certify codec validity or association with any avcC
 * record: production comparison authority is the inventory CARRIED BY a
 * successfully resolved [AvcDecoderConfig]. [copyOf] builds one from arrays;
 * the resolver builds its own from validated ranges.
 */
internal class AvcParameterSetInventory private constructor(
    private val sequence: Array<ByteArray>,
    private val picture: Array<ByteArray>,
) {
    val sequenceParameterSets: Int get() = sequence.size
    val pictureParameterSets: Int get() = picture.size

    fun sequenceParameterSet(index: Int): ByteArray = sequence[index].copyOf()

    fun pictureParameterSet(index: Int): ByteArray = picture[index].copyOf()

    /** Whether bytes [from, to) of [source] are exactly one inventoried set of [type] (7 or 8): same length, every byte. */
    fun matches(type: Int, source: ByteArray, from: Int, to: Int): Boolean {
        val sets = when (type) {
            SEQUENCE_PARAMETER_SET -> sequence
            PICTURE_PARAMETER_SET -> picture
            else -> return false
        }
        return sets.any { set -> set.size == to - from && set.indices.all { set[it] == source[from + it] } }
    }

    internal companion object {
        const val SEQUENCE_PARAMETER_SET = 7
        const val PICTURE_PARAMETER_SET = 8

        /** One copy of each given array, after the counts and lengths are checked. */
        fun copyOf(sequenceParameterSets: List<ByteArray>, pictureParameterSets: List<ByteArray>): AvcParameterSetInventory {
            preflight(sequenceParameterSets.size, { sequenceParameterSets[it].size }, pictureParameterSets.size, { pictureParameterSets[it].size })
            return AvcParameterSetInventory(
                Array(sequenceParameterSets.size) { sequenceParameterSets[it].copyOf() },
                Array(pictureParameterSets.size) { pictureParameterSets[it].copyOf() },
            )
        }

        /**
         * One copy of each range, straight from the resolver's validated
         * units: [sequenceRanges] holds (from, to) pairs into [sequenceSource],
         * [pictureRanges] into [pictureSource]; [sequenceCount] and
         * [pictureCount] pairs are used.
         */
        fun copyOfRanges(
            sequenceSource: ByteArray,
            sequenceRanges: IntArray,
            sequenceCount: Int,
            pictureSource: ByteArray,
            pictureRanges: IntArray,
            pictureCount: Int,
        ): AvcParameterSetInventory {
            preflight(
                sequenceCount, { sequenceRanges[2 * it + 1] - sequenceRanges[2 * it] },
                pictureCount, { pictureRanges[2 * it + 1] - pictureRanges[2 * it] },
            )
            return AvcParameterSetInventory(
                Array(sequenceCount) { sequenceSource.copyOfRange(sequenceRanges[2 * it], sequenceRanges[2 * it + 1]) },
                Array(pictureCount) { pictureSource.copyOfRange(pictureRanges[2 * it], pictureRanges[2 * it + 1]) },
            )
        }

        private fun preflight(sequenceCount: Int, sequenceLength: (Int) -> Int, pictureCount: Int, pictureLength: (Int) -> Int) {
            require(sequenceCount in 1..AvcConfiguration.MAX_PARAMETER_SETS) { "$sequenceCount SPS; an inventory holds 1 to ${AvcConfiguration.MAX_PARAMETER_SETS}" }
            require(pictureCount in 1..AvcConfiguration.MAX_PARAMETER_SETS) { "$pictureCount PPS; an inventory holds 1 to ${AvcConfiguration.MAX_PARAMETER_SETS}" }
            for (i in 0 until sequenceCount) {
                require(sequenceLength(i) in 1..AvcConfiguration.MAX_PARAMETER_SET_BYTES) { "SPS $i is ${sequenceLength(i)} bytes" }
            }
            for (i in 0 until pictureCount) {
                require(pictureLength(i) in 1..AvcConfiguration.MAX_PARAMETER_SET_BYTES) { "PPS $i is ${pictureLength(i)} bytes" }
            }
        }
    }
}

/**
 * The one place the converted payload is allocated, so the bound can be seen
 * to come first. It must return a fresh array of exactly the requested size;
 * any other size is a contract failure, not a verdict.
 */
internal fun interface AvcOutputAllocator {
    fun allocate(size: Int): ByteArray

    companion object {
        val HEAP: AvcOutputAllocator = AvcOutputAllocator { ByteArray(it) }
    }
}

/**
 * Input that violates a cited requirement: ITU-T H.264 (05/2003) 7.3.1/7.4.1
 * (NAL header, nal_ref_idc, emulation prevention) or Annex B.1.2 (zero_byte
 * before an access unit's first NAL unit and before a parameter set).
 * [offset] is the byte offset of the NAL unit's start code in the frame.
 */
internal sealed interface AvcPayloadDefect {
    val description: String

    /** The frame does not begin with a start code (nor with zero bytes before one). */
    data object MissingStartCode : AvcPayloadDefect {
        override val description: String get() = "the frame does not begin with a start code"
    }

    /** The frame begins with 00 00 01: an access unit's first NAL unit needs the zero_byte. */
    data object ThreeByteAccessUnitStart : AvcPayloadDefect {
        override val description: String get() = "the frame begins with a three-byte start code; an access unit's first NAL unit needs 00 00 00 01"
    }

    /** A parameter set after a 00 00 01 prefix: it needs the zero_byte wherever it appears. */
    data class ThreeByteParameterSetStart(val offset: Int) : AvcPayloadDefect {
        override val description: String get() = "at $offset: a parameter set after a three-byte start code needs 00 00 00 01"
    }

    data class EmptyNal(val offset: Int) : AvcPayloadDefect {
        override val description: String get() = "at $offset: an empty NAL unit"
    }

    data class ForbiddenBitSet(val offset: Int) : AvcPayloadDefect {
        override val description: String get() = "at $offset: forbidden_zero_bit is 1"
    }

    /** nal_ref_idc 0 on an IDR slice or a parameter set, or non-zero on SEI or an access unit delimiter. */
    data class ReferenceIndication(val offset: Int, val type: Int, val nalRefIdc: Int) : AvcPayloadDefect {
        override val description: String get() = "at $offset: nal_ref_idc $nalRefIdc is not allowed on NAL unit type $type"
    }

    /** 00 00 00, 00 00 02, or 00 00 03 followed by a byte above 03, inside a NAL unit. */
    data class EmulationViolation(val offset: Int) : AvcPayloadDefect {
        override val description: String get() = "at $offset: a NAL unit breaks the emulation-prevention rules"
    }
}

/** Input this sample does not convert, whether or not it is valid elsewhere: its subset policy, not a claim of malformation. */
internal sealed interface AvcPayloadRefusal {
    val description: String

    /** Zero bytes before the first start code. */
    data object LeadingZeroBytes : AvcPayloadRefusal {
        override val description: String get() = "zero bytes before the frame's first start code"
    }

    /** Zero bytes after a NAL unit (trailing_zero_8bits). Refused, never dropped. */
    data class TrailingZeroBytes(val offset: Int) : AvcPayloadRefusal {
        override val description: String get() = "the NAL unit at $offset is followed by zero bytes"
    }

    /** A NAL unit type neither converted (1, 5, 6, 9) nor a parameter set (7, 8). */
    data class NalUnitType(val offset: Int, val type: Int) : AvcPayloadRefusal {
        override val description: String get() = "at $offset: NAL unit type $type is not converted by this sample"
    }

    /**
     * A parameter set that is not byte-identical to one of the resolved
     * configuration's sets of its type. Not a claim of malformed H.264 or of
     * a proven reconfiguration: the sample only strips what it can prove is
     * already in the description.
     */
    data class UnknownParameterSet(val offset: Int, val type: Int) : AvcPayloadRefusal {
        override val description: String get() = "at $offset: a type $type parameter set that is not in the resolved configuration"
    }

    /** A one-byte retained NAL unit: its four-byte length would be 1, which LOC-01 2.1.3 reads as a start code. */
    data class LengthOneAmbiguous(val offset: Int) : AvcPayloadRefusal {
        override val description: String get() = "at $offset: a one-byte NAL unit would carry a length of 1, which reads as a start code"
    }

    /** More NAL units than the descriptor bound, stripped ones included; refused on observing the first one over. */
    data class TooManyNalUnits(val count: Int, val limit: Int) : AvcPayloadRefusal {
        override val description: String get() = "$count NAL units exceed the limit of $limit"
    }

    /** Nothing a decoder would present remains: no type 1 or 5 slice. Never an empty or control-only video frame. */
    data object NoVideoSlice : AvcPayloadRefusal {
        override val description: String get() = "the frame retains no coded slice (type 1 or 5)"
    }

    /**
     * The exact converted size exceeds the output bound. No converted output
     * was requested from the allocator or published; the input's accessor
     * copy and the descriptors had already been made.
     */
    data class OutputTooLarge(val needed: Long, val limit: Int) : AvcPayloadRefusal {
        override val description: String get() = "the converted payload needs $needed bytes; the limit is $limit"
    }
}

internal sealed interface AvcPayloadVerdict {
    /**
     * The avc1 payload: each RETAINED NAL unit, in order, after its big-endian
     * four-byte length. Identical in-band parameter sets were stripped:
     * [strippedParameterSetBytes] is the sum of their NAL unit lengths, each
     * header included and start codes excluded. Conversion accounting, not
     * evidence of a send.
     */
    class Converted internal constructor(
        val payload: MediaBytes,
        nalUnitTypes: List<Int>,
        val strippedSequenceParameterSets: Int,
        val strippedPictureParameterSets: Int,
        val strippedParameterSetBytes: Long,
    ) : AvcPayloadVerdict {
        private val types: IntArray = nalUnitTypes.toIntArray()

        /** The retained NAL unit types in order: a fresh list on every read, never the holder's own state. */
        val nalUnitTypes: List<Int> get() = types.toList()
    }

    class Malformed(val defect: AvcPayloadDefect) : AvcPayloadVerdict

    class OutsideSubset(val refusal: AvcPayloadRefusal) : AvcPayloadVerdict
}

/**
 * The sample's AVC payload conversion: one frame's Annex-B bytes to the avc1
 * payload of four-byte big-endian lengths. It is meant for media frames:
 * configuration buffers and EMPTY end-of-stream buffers never become frames
 * (EncodedOutputs.decide), while a NON-empty terminal frame is media and is
 * converted like any other. That routing is a future integration obligation;
 * this function takes bytes, not a frame decision.
 *
 * Parameter sets byte-identical to the [config]'s are stripped and counted;
 * any other is refused, and the whole frame with it. Every retained NAL unit
 * keeps its exact bytes and order. Nothing is published, and no output is
 * requested from [output], unless the whole frame converts within [limits].
 * Synchronous and catch-free: an allocation failure leaves as itself.
 *
 * It certifies framing and parameter-set identity only: not one access unit
 * per encoder output, slice or picture semantics, random access, dimensions
 * or bitrate.
 */
internal object AvcPayloads {
    /** The retained NAL unit types: non-IDR slice, IDR slice, SEI, access unit delimiter. */
    val CONVERTED_TYPES: Set<Int> = setOf(1, 5, 6, 9)

    fun convert(
        frame: MediaBytes,
        config: AvcParameterSetInventory,
        limits: AvcPayloadLimits,
        output: AvcOutputAllocator = AvcOutputAllocator.HEAP,
    ): AvcPayloadVerdict {
        val b = frame.bytes()
        // The frame begins exactly with 00 00 00 01.
        if (!isStartCode(b, 0)) {
            var zeros = 0
            while (zeros < b.size && b[zeros].toInt() == 0) zeros++
            return when {
                zeros > 3 && zeros < b.size && b[zeros].toInt() == 1 -> AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.LeadingZeroBytes)
                zeros == 2 && zeros < b.size && b[zeros].toInt() == 1 -> AvcPayloadVerdict.Malformed(AvcPayloadDefect.ThreeByteAccessUnitStart)
                else -> AvcPayloadVerdict.Malformed(AvcPayloadDefect.MissingStartCode)
            }
        }

        // Every NAL unit after the first takes at least four input bytes (a
        // three-byte prefix and a header), so the input bounds how many can
        // be retained no matter how large the configured limit is.
        val capacity = minOf(limits.maxNalUnits.toLong(), b.size.toLong() / 4 + 1).toInt()
        val ranges = IntArray(Math.multiplyExact(capacity, 2))
        val types = IntArray(capacity)
        var retained = 0
        var slices = 0
        var strippedSequence = 0
        var strippedPicture = 0
        var strippedBytes = 0L
        var outputSize = 0L
        var units = 0
        var pos = 0
        var threeBytePrefix = false
        while (true) {
            if (++units > limits.maxNalUnits) {
                return AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.TooManyNalUnits(units, limits.maxNalUnits))
            }
            val start = pos + if (threeBytePrefix) 3 else 4
            val prefix = findPrefix(b, start)
            // The zero run before the next prefix (or the end): one zero_byte
            // belongs to a following start code; any other zero is trailing.
            val runEnd = if (prefix < 0) b.size else prefix
            var zeros = 0
            while (runEnd - zeros > start && b[runEnd - zeros - 1].toInt() == 0) zeros++
            val end = runEnd - zeros
            if (end == start) return AvcPayloadVerdict.Malformed(AvcPayloadDefect.EmptyNal(pos))
            val trailing = if (prefix < 0) zeros else zeros - 1
            if (trailing > 0) return AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.TrailingZeroBytes(pos))

            val header = b[start].toInt() and 0xff
            if (header and 0x80 != 0) return AvcPayloadVerdict.Malformed(AvcPayloadDefect.ForbiddenBitSet(pos))
            val type = header and 0x1f
            val parameterSet =
                type == AvcParameterSetInventory.SEQUENCE_PARAMETER_SET || type == AvcParameterSetInventory.PICTURE_PARAMETER_SET
            if (!parameterSet && type !in CONVERTED_TYPES) return AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.NalUnitType(pos, type))
            if (parameterSet && threeBytePrefix) return AvcPayloadVerdict.Malformed(AvcPayloadDefect.ThreeByteParameterSetStart(pos))
            val nalRefIdc = header ushr 5 and 0x3
            val refIdcAllowed = when (type) {
                IDR_SLICE, AvcParameterSetInventory.SEQUENCE_PARAMETER_SET, AvcParameterSetInventory.PICTURE_PARAMETER_SET -> nalRefIdc != 0
                SEI, ACCESS_UNIT_DELIMITER -> nalRefIdc == 0
                else -> true
            }
            if (!refIdcAllowed) return AvcPayloadVerdict.Malformed(AvcPayloadDefect.ReferenceIndication(pos, type, nalRefIdc))
            if (breaksEmulation(b, start, end)) return AvcPayloadVerdict.Malformed(AvcPayloadDefect.EmulationViolation(pos))

            if (parameterSet) {
                if (!config.matches(type, b, start, end)) {
                    return AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.UnknownParameterSet(pos, type))
                }
                if (type == AvcParameterSetInventory.SEQUENCE_PARAMETER_SET) strippedSequence++ else strippedPicture++
                strippedBytes += end - start
            } else {
                if (end - start == 1) return AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.LengthOneAmbiguous(pos))
                check(retained < capacity) { "more retained NAL units than the input can hold" }
                ranges[2 * retained] = start
                ranges[2 * retained + 1] = end
                types[retained] = type
                retained++
                if (type == IDR_SLICE || type == NON_IDR_SLICE) slices++
                outputSize += NAL_LENGTH_SIZE + (end - start)
            }

            if (prefix < 0) break
            threeBytePrefix = zeros == 0
            pos = if (threeBytePrefix) prefix else prefix - 1
        }

        if (slices == 0) return AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.NoVideoSlice)
        if (outputSize > limits.maxOutputBytes) {
            return AvcPayloadVerdict.OutsideSubset(AvcPayloadRefusal.OutputTooLarge(outputSize, limits.maxOutputBytes))
        }

        // Every refusal is behind us: the one output request, of the exact size.
        val size = outputSize.toInt()
        val out = output.allocate(size)
        check(out.size == size) { "the allocator returned ${out.size} bytes for a request of $size" }
        var at = 0
        for (k in 0 until retained) {
            val from = ranges[2 * k]
            val to = ranges[2 * k + 1]
            val length = to - from
            out[at] = (length ushr 24).toByte()
            out[at + 1] = (length ushr 16).toByte()
            out[at + 2] = (length ushr 8).toByte()
            out[at + 3] = length.toByte()
            b.copyInto(out, at + NAL_LENGTH_SIZE, from, to)
            at += NAL_LENGTH_SIZE + length
        }
        return AvcPayloadVerdict.Converted(
            MediaBytes(out),
            types.asList().subList(0, retained),
            strippedSequence,
            strippedPicture,
            strippedBytes,
        )
    }

    private const val NON_IDR_SLICE = 1
    private const val IDR_SLICE = 5
    private const val SEI = 6
    private const val ACCESS_UNIT_DELIMITER = 9
    private const val NAL_LENGTH_SIZE = 4

    private fun isStartCode(b: ByteArray, at: Int): Boolean =
        at + 4 <= b.size && b[at].toInt() == 0 && b[at + 1].toInt() == 0 && b[at + 2].toInt() == 0 && b[at + 3].toInt() == 1

    /** The index of the next 00 00 01 at or after [from], or -1. */
    private fun findPrefix(b: ByteArray, from: Int): Int {
        var i = from
        while (i + 3 <= b.size) {
            if (b[i].toInt() == 0 && b[i + 1].toInt() == 0 && b[i + 2].toInt() == 1) return i
            i++
        }
        return -1
    }

    /** 00 00 00, 00 00 02, or 00 00 03 followed by a byte above 03, inside [start, end). */
    private fun breaksEmulation(b: ByteArray, start: Int, end: Int): Boolean {
        var i = start
        while (i + 3 <= end) {
            if (b[i].toInt() == 0 && b[i + 1].toInt() == 0) {
                val third = b[i + 2].toInt() and 0xff
                if (third == 0 || third == 2) return true
                if (third == 3 && i + 4 <= end && (b[i + 3].toInt() and 0xff) > 3) return true
            }
            i++
        }
        return false
    }
}
