package org.openmoq.moq5.samples.publisher

import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.CodecSignaling
import org.openmoq.moq5.CodecSignalingException
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.SampleEntry

/** A utility call's outcome: its value, or the code of a refusal of its input. */
internal sealed interface UtilityOutcome<out T> {
    class Done<out T>(val value: T) : UtilityOutcome<T>

    class Refused(val code: MoqResultCode) : UtilityOutcome<Nothing>
}

/**
 * The public codec utility exactly as the AVC configuration step calls it:
 * the two public [CodecSignaling] operations, with a refusal of the input
 * (and only that) as an outcome. Anything else the utility throws is not a
 * refusal and leaves as itself.
 */
internal interface AvcRecordUtility {
    fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): UtilityOutcome<MediaBytes>

    fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes): UtilityOutcome<String>
}

/**
 * The production utility: the public binding, through its process-wide
 * native owner. Only [CodecSignalingException] -- the binding's refusal of
 * the input, carrying the native code -- becomes an outcome.
 */
internal object PublicCodecSignaling : AvcRecordUtility {
    override fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): UtilityOutcome<MediaBytes> =
        try {
            UtilityOutcome.Done(CodecSignaling.buildInitData(source, bytes, nalLengthSize))
        } catch (e: CodecSignalingException) {
            UtilityOutcome.Refused(e.code)
        }

    override fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes): UtilityOutcome<String> =
        try {
            UtilityOutcome.Done(CodecSignaling.formatCodecString(format, sampleEntry, decoderConfig))
        } catch (e: CodecSignalingException) {
            UtilityOutcome.Refused(e.code)
        }
}

/**
 * An out-of-band AVC decoder configuration: the avcC record the utility
 * returned and the codec string formatted from THAT record. A successful
 * build certifies the record's structure only -- not full H.264 decoder
 * semantics, picture dimensions, one access unit per encoder output, or any
 * bitrate promise.
 *
 * [parameterSets] holds every parameter set the resolver validated and fed to
 * that build, in declared order and byte for byte: the authority an in-band
 * parameter set is compared with.
 */
internal class AvcDecoderConfig internal constructor(
    val record: MediaBytes,
    val codecString: String,
    val sequenceParameterSets: Int,
    val pictureParameterSets: Int,
    val parameterSets: AvcParameterSetInventory,
)

/**
 * Input that violates a cited requirement: ITU-T H.264 (05/2003) 7.3.1/7.4.1
 * (NAL header, emulation) and Annex B.1.2 (zero_byte before a
 * parameter set), or the Android MediaCodec codec-specific-data layout (each
 * parameter set starts with 00 00 00 01). Positions are the unit and the byte
 * offset of the NAL's start code within it.
 */
internal sealed interface AvcDefect {
    val description: String

    /** The unit does not begin with a start code (and not with zero bytes before one). */
    data class MissingStartCode(val unit: Int) : AvcDefect {
        override val description: String get() = "unit $unit does not begin with a 00 00 00 01 start code"
    }

    /** A 00 00 01 prefix with no zero_byte before a parameter set. */
    data class ThreeByteStartCode(val unit: Int, val offset: Int) : AvcDefect {
        override val description: String get() = "unit $unit at $offset: a three-byte start code where a parameter set needs 00 00 00 01"
    }

    /** A start code followed directly by another start code or the end of the unit. */
    data class EmptyNal(val unit: Int, val offset: Int) : AvcDefect {
        override val description: String get() = "unit $unit at $offset: an empty NAL unit"
    }

    data class ForbiddenBitSet(val unit: Int, val offset: Int) : AvcDefect {
        override val description: String get() = "unit $unit at $offset: forbidden_zero_bit is 1"
    }

    /** nal_ref_idc 0 on a sequence or picture parameter set. */
    data class ZeroReferenceIndication(val unit: Int, val offset: Int, val nalUnitType: Int) : AvcDefect {
        override val description: String get() = "unit $unit at $offset: nal_ref_idc 0 on a parameter set of type $nalUnitType"
    }

    /**
     * Inside a NAL unit: 00 00 00 or 00 00 02, or 00 00 03 followed by a byte
     * other than 00, 01, 02 or 03 (7.4.1). At a position the three-byte check
     * comes first. A NAL that ends 00 00 03 is not refused: the four-byte rule
     * constrains four-byte sequences only, and no RBSP is validated here.
     */
    data class EmulationViolation(val unit: Int, val offset: Int) : AvcDefect {
        override val description: String get() = "unit $unit at $offset: a NAL unit breaks the emulation-prevention rules"
    }
}

/**
 * Input this sample does not accept, whether or not it is valid elsewhere.
 * These are the sample's subset policy, not claims of malformation.
 */
internal sealed interface AvcSubsetRefusal {
    val description: String

    /** The sample reads csd-0 (sequence parameter sets) and csd-1 (picture parameter sets): exactly two units. */
    data class UnitCount(val count: Int) : AvcSubsetRefusal {
        override val description: String get() = "$count configuration units; the sample reads csd-0 and csd-1"
    }

    /** Zero bytes before the first start code. */
    data class LeadingZeroBytes(val unit: Int) : AvcSubsetRefusal {
        override val description: String get() = "unit $unit has zero bytes before its first start code"
    }

    /** Zero bytes after a NAL unit (trailing_zero_8bits). */
    data class TrailingZeroBytes(val unit: Int, val offset: Int) : AvcSubsetRefusal {
        override val description: String get() = "unit $unit: the NAL at $offset is followed by zero bytes"
    }

    /** A NAL unit type other than a parameter set, including reserved and unspecified types. */
    data class NalUnitType(val unit: Int, val offset: Int, val type: Int) : AvcSubsetRefusal {
        override val description: String get() = "unit $unit at $offset: NAL unit type $type is not a parameter set"
    }

    /** A picture parameter set in csd-0, or a sequence parameter set in csd-1. */
    data class MisplacedParameterSet(val unit: Int, val offset: Int, val type: Int) : AvcSubsetRefusal {
        override val description: String get() = "unit $unit at $offset: a type $type parameter set in the wrong unit"
    }

    /** A parameter set byte-identical to an earlier one. */
    data class DuplicateParameterSet(val unit: Int, val offset: Int) : AvcSubsetRefusal {
        override val description: String get() = "unit $unit at $offset: a parameter set repeated byte for byte"
    }

    /** More parameter sets of one type than the codec utility accepts: refused on observing the first one over, so [count] is the count observed then, not a total. */
    data class TooManyParameterSets(val type: Int, val count: Int, val limit: Int) : AvcSubsetRefusal {
        override val description: String get() = "$count type $type parameter sets exceed the utility's $limit"
    }

    /** A parameter set longer than an avcC length field holds. */
    data class ParameterSetTooLong(val unit: Int, val offset: Int, val length: Int, val limit: Int) : AvcSubsetRefusal {
        override val description: String get() = "unit $unit at $offset: a $length-byte parameter set exceeds $limit"
    }
}

internal enum class UtilityStage { BUILD, FORMAT }

internal sealed interface AvcConfigVerdict {
    class Resolved(val config: AvcDecoderConfig) : AvcConfigVerdict

    class Malformed(val defect: AvcDefect) : AvcConfigVerdict

    class OutsideSubset(val refusal: AvcSubsetRefusal) : AvcConfigVerdict

    /** The public utility refused its input at [stage] with [code]; the exception itself is not retained. */
    class UtilityRefused(val stage: UtilityStage, val code: MoqResultCode) : AvcConfigVerdict
}

/**
 * The sample's AVC configuration step: an owned [RawConfigSnapshot] of
 * csd-0 and csd-1 to an avc1 decoder configuration, through the public codec
 * utility. Synchronous and catch-free: a refusal arrives as a
 * [UtilityOutcome.Refused], so a contract failure, an unavailable native
 * bridge, an allocation failure or a cancellation leaves as itself.
 */
internal object AvcConfiguration {
    /** The codec utility's own limit on parameter sets of one type. */
    const val MAX_PARAMETER_SETS = 16

    /** An avcC parameter-set length field is 16 bits. */
    const val MAX_PARAMETER_SET_BYTES = 0xffff

    /** The NAL length size the avc1 record declares. */
    const val NAL_LENGTH_SIZE = 4

    val AVC1: SampleEntry = SampleEntry("avc1")

    fun resolve(config: RawConfigSnapshot, utility: AvcRecordUtility = PublicCodecSignaling): AvcConfigVerdict {
        if (config.unitCount != 2) return AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.UnitCount(config.unitCount))
        val units = arrayOf(config.unit(0), config.unit(1))
        val counts = IntArray(2)
        val ranges = arrayOf(IntArray(2 * MAX_PARAMETER_SETS), IntArray(2 * MAX_PARAMETER_SETS))
        for (u in 0..1) {
            val refusal = scanUnit(u, units[u], SEQUENCE_PARAMETER_SET + u, ranges[u]) { ++counts[u] }
            if (refusal != null) return refusal
        }

        // Each unit is now exactly `00 00 00 01 NAL` sequences, validated on
        // its own, so the Annex-B feed is csd-0 followed by csd-1, byte for byte.
        val feedSize = units[0].size.toLong() + units[1].size.toLong()
        check(feedSize <= Int.MAX_VALUE) { "a feed of $feedSize bytes exceeds an array" }
        val feed = units[0] + units[1]
        val record =
            when (val built = utility.buildInitData(CodecConfigSource.AvcAnnexB, MediaBytes(feed), NAL_LENGTH_SIZE)) {
                is UtilityOutcome.Refused -> return AvcConfigVerdict.UtilityRefused(UtilityStage.BUILD, built.code)
                is UtilityOutcome.Done -> built.value
            }
        val codecString =
            when (val formatted = utility.formatCodecString(CodecConfigFormat.AvcRecord, AVC1, record)) {
                is UtilityOutcome.Refused -> return AvcConfigVerdict.UtilityRefused(UtilityStage.FORMAT, formatted.code)
                is UtilityOutcome.Done -> formatted.value
            }
        // One copy of each validated range: the units are this call's own copies.
        val parameterSets = AvcParameterSetInventory.copyOfRanges(units[0], ranges[0], counts[0], units[1], ranges[1], counts[1])
        return AvcConfigVerdict.Resolved(AvcDecoderConfig(record, codecString, counts[0], counts[1], parameterSets))
    }

    private const val SEQUENCE_PARAMETER_SET = 7

    /**
     * Scans one unit for [expectedType] parameter sets. Positions are byte
     * offsets of each NAL's four-byte start code. Each accepted NAL's
     * (start, end) is kept in [kept] -- at most [MAX_PARAMETER_SETS], for the
     * duplicate check and the caller's inventory; the next one over is
     * refused on sight. [accept] counts an accepted NAL and returns the count
     * so far. Returns the first finding, or null.
     */
    private fun scanUnit(u: Int, b: ByteArray, expectedType: Int, kept: IntArray, accept: () -> Int): AvcConfigVerdict? {
        // The unit begins exactly with 00 00 00 01.
        if (!isStartCode(b, 0)) {
            var zeros = 0
            while (zeros < b.size && b[zeros].toInt() == 0) zeros++
            return when {
                zeros > 3 && zeros < b.size && b[zeros].toInt() == 1 ->
                    AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.LeadingZeroBytes(u))
                zeros == 2 && zeros < b.size && b[zeros].toInt() == 1 ->
                    AvcConfigVerdict.Malformed(AvcDefect.ThreeByteStartCode(u, 0))
                else -> AvcConfigVerdict.Malformed(AvcDefect.MissingStartCode(u))
            }
        }
        var keptCount = 0
        var pos = 0
        while (true) {
            val start = pos + 4
            val prefix = findPrefix(b, start)
            // The zero run before the next prefix (or the end): one zero_byte
            // belongs to a following start code; any other zero is trailing.
            val runEnd = if (prefix < 0) b.size else prefix
            var zeros = 0
            while (runEnd - zeros > start && b[runEnd - zeros - 1].toInt() == 0) zeros++
            if (prefix >= 0 && zeros == 0) return AvcConfigVerdict.Malformed(AvcDefect.ThreeByteStartCode(u, prefix))
            val end = runEnd - zeros
            if (end == start) return AvcConfigVerdict.Malformed(AvcDefect.EmptyNal(u, pos))
            val trailing = if (prefix < 0) zeros else zeros - 1
            if (trailing > 0) return AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.TrailingZeroBytes(u, pos))

            val header = b[start].toInt() and 0xff
            if (header and 0x80 != 0) return AvcConfigVerdict.Malformed(AvcDefect.ForbiddenBitSet(u, pos))
            val type = header and 0x1f
            if (type != SEQUENCE_PARAMETER_SET && type != SEQUENCE_PARAMETER_SET + 1) {
                return AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.NalUnitType(u, pos, type))
            }
            if (type != expectedType) return AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.MisplacedParameterSet(u, pos, type))
            if (header ushr 5 and 0x3 == 0) return AvcConfigVerdict.Malformed(AvcDefect.ZeroReferenceIndication(u, pos, type))
            if (breaksEmulation(b, start, end)) return AvcConfigVerdict.Malformed(AvcDefect.EmulationViolation(u, pos))
            val length = end - start
            if (length > MAX_PARAMETER_SET_BYTES) {
                return AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.ParameterSetTooLong(u, pos, length, MAX_PARAMETER_SET_BYTES))
            }
            for (k in 0 until keptCount) {
                if (sameRange(b, kept[2 * k], kept[2 * k + 1], start, end)) {
                    return AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.DuplicateParameterSet(u, pos))
                }
            }
            val count = accept()
            if (count > MAX_PARAMETER_SETS) {
                return AvcConfigVerdict.OutsideSubset(AvcSubsetRefusal.TooManyParameterSets(type, count, MAX_PARAMETER_SETS))
            }
            kept[2 * keptCount] = start
            kept[2 * keptCount + 1] = end
            keptCount++
            if (prefix < 0) return null
            pos = prefix - 1
        }
    }

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

    private fun sameRange(b: ByteArray, aFrom: Int, aTo: Int, bFrom: Int, bTo: Int): Boolean {
        if (aTo - aFrom != bTo - bFrom) return false
        for (i in 0 until aTo - aFrom) if (b[aFrom + i] != b[bFrom + i]) return false
        return true
    }
}
