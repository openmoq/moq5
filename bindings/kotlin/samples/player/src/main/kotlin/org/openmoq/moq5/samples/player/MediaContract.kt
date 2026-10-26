package org.openmoq.moq5.samples.player

import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.UnixMicros

/**
 * The receive-side media contract this sample owns.
 *
 * Every decision is a function of ordinary values, for the same reason
 * discovery is: `MediaTrack` and `MediaObject` have internal constructors, so a
 * consumer cannot build one. The facts below are produced from the PUBLIC parts
 * a consumer can build -- [TrackDescription] and [MediaPayload] -- by the very
 * functions the session will call, so an omitted field cannot hide behind a
 * hand-written fixture.
 *
 * Units are stated, never inferred. A media-timeline position is microseconds
 * ([MediaMicros]); a codec's `presentationTimeUs` is microseconds; they meet
 * with no scaling. A CMAF sample table is in the track's own ticks and converts
 * CUMULATIVELY: `base + (summed ticks) * 1_000_000 / timescale` in 64-bit
 * integer arithmetic. Summing individually truncated durations drifts.
 *
 * The base itself is already rounded: the library reports `decodeTime` in
 * microseconds, so tick-exactness below the microsecond is not recoverable here
 * and is not claimed.
 */
internal object MediaContract {
    /** The NAL start code MediaCodec expects ahead of each Annex-B unit. */
    val START_CODE: ByteArray = byteArrayOf(0, 0, 0, 1)
}

/** How one payload's NALs are framed. */
internal enum class VideoFraming {
    /** Each NAL preceded by its length, of `AvcConfig.nalLengthSize` bytes. */
    LengthPrefixed,

    /** Each NAL preceded by a start code. */
    AnnexB,
}

/**
 * Where the framing answer comes from, which is the PACKAGING -- not a catalog
 * flag (there is none) and not a user guess.
 *
 * A CMAF fragment's samples are length-prefixed by the configuration record,
 * strictly: that is the container's contract. draft-ietf-moq-loc-01 2.1-2.1.4
 * admits BOTH carriages for a RAW/LOC payload and gives the disambiguation
 * rule itself (2.1.3): a leading four-byte value of 1 is the start code, not a
 * NAL length of one. So a RAW payload is disambiguated per object, by that
 * rule, and never by a syncword-style guess.
 */
internal enum class FramingPolicy {
    /** CMAF: always length-prefixed, width from the configuration record. */
    StrictLengthPrefixed,

    /** RAW/LOC: decided per payload by LOC-01 2.1.3. */
    LocDisambiguated,
}

/**
 * How this source carries AAC: raw access units described by an
 * AudioSpecificConfig, or ADTS frames that carry their own header. The
 * WebCodecs AAC registration draws exactly this line; a syncword guess does
 * not, and a CMAF AAC sample is not an ADTS frame.
 *
 * NOTHING IN THE CATALOG CARRIES THIS. It is an explicit application
 * configuration of the sample, defaulting to [Raw]; an ADTS source has to be
 * said out loud. It is not inferred from bytes.
 */
internal enum class AacBitstream {
    /** Raw access units; the AudioSpecificConfig is the codec configuration. */
    Raw,

    /** ADTS frames; each carries a header ahead of its access unit. */
    Adts,
}

/** What the catalog says about one track, plus what the sample declares about its carriage. */
internal data class TrackFacts(
    val mediaType: MediaType?,
    val packaging: PackagingKind?,
    /** The catalog's codec string, e.g. `avc1.64001f` or `mp4a.40.2`. */
    val codec: String?,
    /**
     * The codec configuration: an AVCDecoderConfigurationRecord for AVC, an
     * AudioSpecificConfig for AAC.
     */
    val codecConfig: ByteArray?,
    /** The track's tick rate; a CMAF sample table is expressed in these ticks. */
    val timescale: UInt = 0u,
    val width: UInt = 0u,
    val height: UInt = 0u,
    val samplerate: UInt = 0u,
    val channelCount: UInt = 0u,
    /** The application's declared AAC carriage; nothing in the catalog says. */
    val aacBitstream: AacBitstream = AacBitstream.Raw,
) {
    // Value semantics with the array compared by CONTENT: an oracle that
    // compared array references would pass on any two distinct copies.
    override fun equals(other: Any?): Boolean =
        other is TrackFacts && mediaType == other.mediaType && packaging == other.packaging &&
            codec == other.codec && timescale == other.timescale &&
            width == other.width && height == other.height &&
            samplerate == other.samplerate && channelCount == other.channelCount &&
            aacBitstream == other.aacBitstream &&
            (codecConfig?.contentEquals(other.codecConfig) ?: (other.codecConfig == null))

    override fun hashCode(): Int =
        listOf<Any?>(
            mediaType, packaging, codec, timescale, width, height, samplerate, channelCount,
            aacBitstream,
        ).fold(codecConfig?.contentHashCode() ?: 0) { acc, v -> acc * 31 + v.hashCode() }
}

/** The AVC decoder configuration, as carried in the sample's own terms. */
internal data class AvcConfig(
    /** Parameter sets in record order, each WITHOUT a start code. */
    val sequenceParameterSets: List<ByteArray>,
    val pictureParameterSets: List<ByteArray>,
    /** `lengthSizeMinusOne + 1`: the width of a length-prefixed NAL's prefix. */
    val nalLengthSize: Int,
    val profile: Int,
    val constraints: Int,
    val level: Int,
)

/** The AAC decoder configuration (AudioSpecificConfig). */
internal data class AacConfig(
    val audioObjectType: Int,
    val samplingFrequency: Int,
    val channelCount: Int,
)

/**
 * The decoder format the sample would configure, or the exact reason it will
 * not. An unsupported track is a NAMED refusal: it never becomes a decoder
 * that is fed bytes it cannot read.
 */
internal sealed interface FormatOutcome {
    data class Video(
        /** The MediaCodec MIME type, e.g. `video/avc`. */
        val mime: String,
        val width: Int,
        val height: Int,
        /** `csd-0`/`csd-1` in that order, each already Annex-B framed. */
        val csd: List<ByteArray>,
        val avc: AvcConfig,
        val packaging: PackagingKind,
        val framing: FramingPolicy,
        val timescale: UInt,
    ) : FormatOutcome

    data class Audio(
        val mime: String,
        val csd: List<ByteArray>,
        val aac: AacConfig,
        val packaging: PackagingKind,
        val bitstream: AacBitstream,
        val timescale: UInt,
    ) : FormatOutcome

    data class Unsupported(val reason: String) : FormatOutcome
}

/**
 * One sample of a CMAF fragment's sample table, in the track's own ticks and in
 * the public widths (`CmafSample` carries `UInt`/`Int`). Nothing is narrowed
 * here; the checked narrowing happens where the arithmetic does.
 */
internal data class SampleFacts(
    val sizeBytes: UInt,
    val durationTicks: UInt,
    /** Composition offset, legitimately SIGNED. */
    val compositionOffsetTicks: Int,
    /** The trun sample flags: each sample's own sync classification lives here. */
    val flags: UInt,
)

/** One received object, as the sample reads it. */
internal data class ObjectFacts(
    /**
     * The media bytes: the RAW payload, or the CMAF fragment's mdat span. A
     * status object carries none.
     */
    val payload: ByteArray,
    val status: ObjectStatus,
    /**
     * The object-level keyframe fact. For CMAF video the library derives this
     * from the FIRST sample only, so it is not each sample's classification.
     */
    val keyframe: Boolean,
    val presentationTime: MediaMicros,
    val decodeTime: MediaMicros,
    /** The object-level composition offset, legitimately SIGNED. */
    val compositionOffset: MediaOffsetMicros,
    /**
     * The LOC capture timestamp AS REPORTED, or `null` when the object carried
     * none. Presence is a fact of its own: a genuine value of zero is NOT
     * absence, and absence is never inferred from a magnitude.
     */
    val captureTime: UnixMicros?,
    /**
     * The fragment's sample table for a CMAF object. A normal CMAF object
     * always has at least one sample -- the library refuses a zero-sample
     * fragment -- and an empty table belongs to a status object.
     */
    val samples: List<SampleFacts> = emptyList(),
) {
    override fun equals(other: Any?): Boolean =
        other is ObjectFacts && status == other.status && keyframe == other.keyframe &&
            presentationTime == other.presentationTime && decodeTime == other.decodeTime &&
            compositionOffset == other.compositionOffset && captureTime == other.captureTime &&
            samples == other.samples &&
            payload.contentEquals(other.payload)

    override fun hashCode(): Int =
        listOf<Any?>(status, keyframe, presentationTime, decodeTime, compositionOffset, captureTime, samples)
            .fold(payload.contentHashCode()) { acc, v -> acc * 31 + v.hashCode() }
}

/**
 * What one access unit becomes. [StatusOnly] is not an empty frame: an
 * end-of-group object carries no media at all and is never queued.
 */
internal sealed interface AccessUnitOutcome {
    data class Unit(
        /** Exactly the bytes to queue, in the decoder's expected framing. */
        val bytes: ByteArray,
        /** `presentationTimeUs`, the media timeline in microseconds. */
        val presentationTimeUs: Long,
        val sync: Boolean,
    ) : AccessUnitOutcome {
        override fun equals(other: Any?): Boolean =
            other is Unit && presentationTimeUs == other.presentationTimeUs &&
                sync == other.sync && bytes.contentEquals(other.bytes)

        override fun hashCode(): Int =
            (bytes.contentHashCode() * 31 + presentationTimeUs.hashCode()) * 31 + sync.hashCode()

        override fun toString(): String =
            "Unit(bytes=" + bytes.joinToString("") { "%02x".format(it) } +
                ", presentationTimeUs=" + presentationTimeUs + ", sync=" + sync + ")"
    }

    data object StatusOnly : AccessUnitOutcome

    data class Rejected(val reason: String) : AccessUnitOutcome
}

/**
 * The sample's own mapping from the PUBLIC parts into the facts above. This is
 * the seam the session uses; a test drives the same function, so a dropped
 * field is visible here rather than in a hand-written fixture.
 */
internal object MediaFacts {
    fun ofTrack(
        description: TrackDescription,
        aacBitstream: AacBitstream = AacBitstream.Raw,
    ): TrackFacts {
        // A CMAF init box is more specific than the catalog's own fields, so it
        // wins where it says anything; nothing is invented where neither does.
        val init = description.init
        fun pick(fromInit: UInt, fromCatalog: UInt?): UInt = if (fromInit != 0u) fromInit else (fromCatalog ?: 0u)
        return TrackFacts(
            mediaType = description.mediaType,
            packaging = description.packagingKind,
            codec = description.codec?.utf8OrNull(),
            codecConfig = (init?.codecConfig ?: description.initData)?.bytes(),
            timescale = if (init != null && init.timescale != 0u) init.timescale else description.timescale,
            width = pick(init?.width ?: 0u, description.catalogWidth),
            height = pick(init?.height ?: 0u, description.catalogHeight),
            samplerate = pick(init?.samplerate ?: 0u, description.catalogSamplerate),
            channelCount = pick(init?.channelCount ?: 0u, description.channelConfig?.utf8OrNull()?.toUIntOrNull()),
            aacBitstream = aacBitstream,
        )
    }

    fun ofObject(
        payload: MediaPayload,
        status: ObjectStatus,
        keyframe: Boolean,
        presentationTime: MediaMicros,
        decodeTime: MediaMicros,
        compositionOffset: MediaOffsetMicros,
        captureTime: UnixMicros?,
    ): ObjectFacts {
        val bytes: ByteArray
        val samples: List<SampleFacts>
        when (payload) {
            is MediaPayload.Raw -> {
                bytes = payload.payload.bytes()
                samples = emptyList()
            }
            is MediaPayload.Cmaf -> {
                val fragment = payload.fragment.bytes()
                val span = payload.mdat
                require(span.offset + span.length <= fragment.size) {
                    "the mdat span is not inside a " + fragment.size + "-byte fragment"
                }
                bytes = fragment.copyOfRange(span.offset, span.offset + span.length)
                samples =
                    payload.samples.map {
                        SampleFacts(
                            sizeBytes = it.sizeBytes,
                            durationTicks = it.durationTicks,
                            compositionOffsetTicks = it.compositionOffsetTicks,
                            flags = it.flags,
                        )
                    }
            }
        }
        return ObjectFacts(
            payload = bytes,
            status = status,
            keyframe = keyframe,
            presentationTime = presentationTime,
            decodeTime = decodeTime,
            compositionOffset = compositionOffset,
            captureTime = captureTime,
            samples = samples,
        )
    }
}

/**
 * Reads an AVCDecoderConfigurationRecord (ISO/IEC 14496-15 5.3.3.1) and frames
 * NALs for the decoder.
 */
internal object Avc {
    /** Reads the record, or `null` when it is malformed. */
    fun parseConfigOrNull(record: ByteArray): AvcConfig? {
        // ISO/IEC 14496-15 5.3.3.1. Every read is bounds-checked; a record that
        // ends inside a parameter set is malformed, not a short answer.
        if (record.size < 7 || record[0].toInt() != 1) return null
        val nalLengthSize = (record[4].toInt() and 0x03) + 1
        var at = 6
        fun sets(count: Int): List<ByteArray>? {
            val out = ArrayList<ByteArray>(count)
            repeat(count) {
                if (record.size - at < 2) return null
                val length = ((record[at].toInt() and 0xFF) shl 8) or (record[at + 1].toInt() and 0xFF)
                at += 2
                if (length <= 0 || length > record.size - at) return null
                out.add(record.copyOfRange(at, at + length))
                at += length
            }
            return out
        }
        val spsCount = record[5].toInt() and 0x1F
        if (spsCount == 0) return null
        val sps = sets(spsCount) ?: return null
        if (at >= record.size) return null
        val ppsCount = record[at++].toInt() and 0xFF
        if (ppsCount == 0) return null
        val pps = sets(ppsCount) ?: return null
        // The chroma/bit-depth extension is either absent (a legacy record a
        // reader still tolerates) or complete; a record that stops inside it is
        // malformed.
        val profile = record[1].toInt() and 0xFF
        val trailing = record.size - at
        if (profile in EXTENDED_PROFILES && trailing != 0) {
            if (trailing < 4) return null
            at += 3 // chroma format and the two bit depths
            val extensionCount = record[at++].toInt() and 0xFF
            // The count declares length-framed entries, not merely a four-byte
            // tail. Validate every entry even though CSD uses the base SPS/PPS.
            if (sets(extensionCount) == null) return null
        }
        return AvcConfig(
            sequenceParameterSets = sps,
            pictureParameterSets = pps,
            nalLengthSize = nalLengthSize,
            profile = record[1].toInt() and 0xFF,
            constraints = record[2].toInt() and 0xFF,
            level = record[3].toInt() and 0xFF,
        )
    }

    fun parseConfig(record: ByteArray): AvcConfig =
        requireNotNull(parseConfigOrNull(record)) { "not an AVCDecoderConfigurationRecord: " + record.size + " byte(s)" }

    /**
     * The codec string for this record under a GIVEN sample entry. A
     * configuration record does not distinguish `avc1` from `avc3` -- the same
     * record yields either -- so the entry is an input, never inferred. The
     * publisher builder declares `avc1` because it publishes its parameter sets
     * out of band; an incoming `avc3` track is never relabelled.
     */
    fun codecString(config: AvcConfig, sampleEntry: String): String {
        // RFC 6381 3.3: avc1/avc3 with the record's three profile bytes, in
        // lowercase hexadecimal.
        require(sampleEntry == "avc1" || sampleEntry == "avc3") { "not an AVC sample entry: " + sampleEntry }
        return "%s.%02x%02x%02x".format(sampleEntry, config.profile, config.constraints, config.level)
    }

    /**
     * Builds the COMPLETE configuration record from collected parameter sets,
     * including the chroma/bit-depth extension the record carries for the
     * profiles that define it (ISO/IEC 14496-15 5.3.3.1).
     */
    fun buildConfig(sps: List<ByteArray>, pps: List<ByteArray>, nalLengthSize: Int): ByteArray {
        require(nalLengthSize in 1..4) { "a NAL length prefix is one to four bytes; got " + nalLengthSize }
        require(sps.isNotEmpty() && pps.isNotEmpty()) { "a record needs at least one SPS and one PPS" }
        // The record's own count fields are five and eight bits wide.
        require(sps.size <= 31) { "a record carries at most 31 SPS entries; got " + sps.size }
        require(pps.size <= 255) { "a record carries at most 255 PPS entries; got " + pps.size }
        require(sps.none { it.isEmpty() } && pps.none { it.isEmpty() }) {
            "a zero-length parameter set is not a parameter set"
        }
        val first = sps.first()
        require(first.size >= 4) { "an SPS shorter than its own profile bytes cannot be described" }
        val out = ArrayList<Byte>()
        out.add(1)
        out.add(first[1])
        out.add(first[2])
        out.add(first[3])
        out.add(((0xFC or (nalLengthSize - 1)) and 0xFF).toByte())
        out.add(((0xE0 or sps.size) and 0xFF).toByte())
        fun append(sets: List<ByteArray>) {
            for (set in sets) {
                require(set.size <= 0xFFFF) { "a parameter set of " + set.size + " bytes has no 16-bit length" }
                out.add(((set.size shr 8) and 0xFF).toByte())
                out.add((set.size and 0xFF).toByte())
                for (b in set) out.add(b)
            }
        }
        append(sps)
        out.add((pps.size and 0xFF).toByte())
        append(pps)
        val profile = first[1].toInt() and 0xFF
        if (profile in EXTENDED_PROFILES) {
            val sequence = SpsFields.of(first) ?: error("the SPS does not describe its chroma format")
            out.add(((0xFC or sequence.chromaFormatIdc) and 0xFF).toByte())
            out.add(((0xF8 or sequence.bitDepthLumaMinus8) and 0xFF).toByte())
            out.add(((0xF8 or sequence.bitDepthChromaMinus8) and 0xFF).toByte())
            out.add(0) // numOfSequenceParameterSetExt
        }
        return out.toByteArray()
    }

    /**
     * The framing of one RAW/LOC payload, per draft-ietf-moq-loc-01 2.1.3: a
     * leading four-byte `00 00 00 01` is the start code, not a NAL length of
     * one. Never called for CMAF, whose framing is the container's.
     */
    fun framingOf(payload: ByteArray, config: AvcConfig): VideoFraming {
        @Suppress("UNUSED_PARAMETER")
        val unusedWidth = config.nalLengthSize
        return if (payload.size >= 4 && payload[0].toInt() == 0 && payload[1].toInt() == 0 &&
            payload[2].toInt() == 0 && payload[3].toInt() == 1
        ) {
            VideoFraming.AnnexB
        } else {
            VideoFraming.LengthPrefixed
        }
    }

    /** Length-prefixed NALs to Annex-B, or `null` when the framing does not hold. */
    fun toAnnexBOrNull(sample: ByteArray, config: AvcConfig, maxOutputBytes: Int = Int.MAX_VALUE): ByteArray? {
        val width = config.nalLengthSize
        if (width !in 1..4 || maxOutputBytes <= 0) return null
        // Validate the WHOLE framing and its expanded size before allocating.
        // One-byte prefixes may expand; boxed bytes amplify that far further.
        var outputBytes = 0L
        var at = 0
        while (at < sample.size) {
            if (width > sample.size - at) return null
            var length = 0L
            for (b in 0 until width) length = (length shl 8) or (sample[at + b].toLong() and 0xFF)
            at += width
            if (length <= 0L || at + length > sample.size) return null
            outputBytes += 4 + length
            if (outputBytes > maxOutputBytes) return null
            at += length.toInt()
        }
        if (outputBytes == 0L) return null
        val out = ByteArray(outputBytes.toInt())
        at = 0
        var write = 0
        while (at < sample.size) {
            var length = 0
            for (b in 0 until width) length = (length shl 8) or (sample[at + b].toInt() and 0xFF)
            at += width
            MediaContract.START_CODE.copyInto(out, write)
            write += 4
            sample.copyInto(out, write, at, at + length)
            write += length
            at += length
        }
        return out
    }

    fun toAnnexB(sample: ByteArray, config: AvcConfig): ByteArray =
        requireNotNull(toAnnexBOrNull(sample, config)) { "not length-prefixed with " + config.nalLengthSize + "-byte prefixes" }

    /**
     * Copies LOC-selected Annex-B only when each delimiter precedes a nonempty
     * unit. This checks framing, not NAL header/RBSP semantics. Zero bytes
     * before a delimiter or at the end are padding, not a unit of their own;
     * emulation-prevention bytes remain unchanged in the copied payload.
     */
    fun copyAnnexBOrNull(sample: ByteArray): ByteArray? {
        if (sample.size <= MediaContract.START_CODE.size ||
            !MediaContract.START_CODE.indices.all { sample[it] == MediaContract.START_CODE[it] }
        ) return null
        var zeros = 0
        var hasUnitBytes = false
        for (at in MediaContract.START_CODE.size until sample.size) {
            val byte = sample[at].toInt() and 0xFF
            if (byte == 0) {
                zeros++
            } else {
                if (byte == 1 && zeros >= 2) {
                    if (!hasUnitBytes) return null
                    hasUnitBytes = false
                } else {
                    hasUnitBytes = true
                }
                zeros = 0
            }
        }
        return if (hasUnitBytes) sample.copyOf() else null
    }

    /** The profiles whose configuration record carries the chroma/bit-depth extension. */
    private val EXTENDED_PROFILES = setOf(100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135)
}

/**
 * The few SPS fields the configuration record repeats: chroma format and the
 * two bit depths, read as exponential-Golomb codes over the unescaped RBSP.
 */
internal data class SpsFields(
    val chromaFormatIdc: Int,
    val bitDepthLumaMinus8: Int,
    val bitDepthChromaMinus8: Int,
) {
    companion object {
        fun of(nal: ByteArray): SpsFields? {
            if (nal.size < 5) return null
            // Drop the NAL header, then the emulation-prevention bytes.
            val rbsp = ArrayList<Byte>(nal.size)
            var zeros = 0
            for (i in 1 until nal.size) {
                val b = nal[i].toInt() and 0xFF
                if (zeros >= 2 && b == 3) {
                    zeros = 0
                    continue
                }
                rbsp.add(nal[i])
                zeros = if (b == 0) zeros + 1 else 0
            }
            val bits = Bits(rbsp.toByteArray())
            val profile = bits.read(8) ?: return null
            if (bits.read(8) == null) return null // constraint flags
            if (bits.read(8) == null) return null // level
            if (bits.ue() == null) return null // seq_parameter_set_id
            if (profile !in CHROMA_PROFILES) return SpsFields(1, 0, 0)
            val chroma = bits.ue() ?: return null
            if (chroma == 3 && bits.read(1) == null) return null
            val luma = bits.ue() ?: return null
            val chromaDepth = bits.ue() ?: return null
            if (chroma > 3 || luma > 6 || chromaDepth > 6) return null
            return SpsFields(chroma, luma, chromaDepth)
        }

        private val CHROMA_PROFILES = setOf(100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135)
    }

    /** A minimal MSB-first bit reader; every read is bounds-checked. */
    private class Bits(private val bytes: ByteArray) {
        private var at = 0

        fun read(count: Int): Int? {
            var value = 0
            repeat(count) {
                val index = at shr 3
                if (index >= bytes.size) return null
                val bit = (bytes[index].toInt() shr (7 - (at and 7))) and 1
                value = (value shl 1) or bit
                at++
            }
            return value
        }

        fun ue(): Int? {
            var zeros = 0
            while (true) {
                val bit = read(1) ?: return null
                if (bit == 1) break
                zeros++
                // 31 leading zeros would need a 32-bit value: the shift that
                // builds it overflows a signed Int, so it cannot be read.
                if (zeros >= 31) return null
            }
            if (zeros == 0) return 0
            val tail = read(zeros) ?: return null
            return (1 shl zeros) - 1 + tail
        }
    }
}

/** What an AudioSpecificConfig describes, or why this sample will not take it. */
internal sealed interface AacParse {
    data class Ok(val config: AacConfig) : AacParse

    data class No(val reason: String) : AacParse
}

/** Reads an AudioSpecificConfig and the ADTS framing. */
internal object Aac {
    /** ISO/IEC 14496-3 sampling frequency table, by index. */
    private val FREQUENCIES =
        intArrayOf(96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000, 7_350)

    /**
     * ISO/IEC 14496-3 channelConfiguration: 1..6 are that many channels, 7 is
     * 7.1 -- EIGHT channels -- 0 defers to a programme config element, and
     * 8..15 are reserved. The field is not a channel count.
     */
    private fun channelsOrNull(configuration: Int): Int? =
        when (configuration) {
            in 1..6 -> configuration
            7 -> 8
            else -> null
        }

    fun parse(config: ByteArray): AacParse {
        if (config.size < 2) return AacParse.No("an AudioSpecificConfig is at least two bytes")
        val first = config[0].toInt() and 0xFF
        val second = config[1].toInt() and 0xFF
        val objectType = first shr 3
        // 31 is the escape to a six-bit extension; this sample does not read it.
        if (objectType == 31) return AacParse.No("the AAC object-type escape is not read here")
        if (objectType != 2) {
            // AOT 5 (SBR) and friends carry an extension sampling frequency and
            // a decoder configuration this tranche does not derive.
            return AacParse.No("this sample supports AAC-LC only, not audio object type " + objectType)
        }
        val frequencyIndex = ((first and 0x07) shl 1) or (second shr 7)
        if (frequencyIndex >= FREQUENCIES.size) {
            return AacParse.No("reserved or explicit sampling-frequency indices are not supported here")
        }
        val channels = channelsOrNull((second shr 3) and 0x0F)
            ?: return AacParse.No("this channel configuration does not give a channel count here")

        // GASpecificConfig follows the 13-bit ASC header, not a byte boundary.
        // These fields remain in csd-0 for the decoder; this parser derives no
        // frame duration or core-coder timing from them.
        val bits = ConfigBits(config)
        bits.read(1) // frameLengthFlag: both forms keep the same rate/channels.
        if (bits.read(1) == 1 && bits.read(14) == null) return AacParse.No("truncated coreCoderDelay")
        val extensionFlag = bits.read(1) ?: return AacParse.No("missing GASpecificConfig extensionFlag")
        if (extensionFlag == 1) {
            val extensionFlag3 = bits.read(1) ?: return AacParse.No("missing GASpecificConfig extensionFlag3")
            if (extensionFlag3 != 0) return AacParse.No("further GASpecificConfig extensions are not supported here")
        }

        // Backward-compatible SBR signalling can follow an ordinary LC header.
        // SBR present changes decoder/output semantics, so it cannot be silently
        // reported as plain LC. An explicit SBR-absent extension changes none.
        if (!bits.remainingIsZero()) {
            if (bits.read(11) != 0x2b7) return AacParse.No("unknown or truncated AudioSpecificConfig extension")
            val extensionType = bits.read(5) ?: return AacParse.No("missing extension audio object type")
            if (extensionType != 5) return AacParse.No("this sample does not support extension audio object type $extensionType")
            val sbrPresent = bits.read(1) ?: return AacParse.No("missing sbrPresentFlag")
            if (sbrPresent != 0) return AacParse.No("this sample supports AAC-LC only, not SBR")
            if (!bits.remainingIsZero()) {
                // Explicitly absent PS, like absent SBR, changes none of the
                // LC facts. PS present remains outside this sample's subset.
                if (bits.read(11) != 0x548) return AacParse.No("unsupported trailing AudioSpecificConfig data")
                val psPresent = bits.read(1) ?: return AacParse.No("missing psPresentFlag")
                if (psPresent != 0) return AacParse.No("this sample supports AAC-LC only, not PS")
                if (!bits.remainingIsZero()) return AacParse.No("unsupported trailing AudioSpecificConfig data")
            }
        }
        return AacParse.Ok(AacConfig(objectType, FREQUENCIES[frequencyIndex], channels))
    }

    /** Bounded MSB-first reads after the ASC header; Long avoids byte-count overflow. */
    private class ConfigBits(private val bytes: ByteArray) {
        private val total = bytes.size.toLong() * 8
        private var position = 13L

        fun read(width: Int): Int? {
            if (total - position < width) return null
            var value = 0
            repeat(width) {
                value = (value shl 1) or bitAt(position)
                position++
            }
            return value
        }

        fun remainingIsZero(): Boolean {
            var at = position
            while (at < total) {
                if (bitAt(at) != 0) return false
                at++
            }
            return true
        }

        private fun bitAt(at: Long): Int =
            (bytes[(at ushr 3).toInt()].toInt() ushr (7 - (at and 7).toInt())) and 1
    }

    fun parseConfigOrNull(config: ByteArray): AacConfig? = (parse(config) as? AacParse.Ok)?.config

    fun parseConfig(config: ByteArray): AacConfig =
        requireNotNull(parseConfigOrNull(config)) { "not an AAC-LC AudioSpecificConfig: " + config.size + " byte(s)" }

    /**
     * The raw access unit inside an ADTS frame, or `null` when the framing does
     * not hold or the frame contains multiple raw_data_blocks. This converter
     * returns one access unit, not a concatenation or CRC/position framing.
     * Only called for an explicitly ADTS-configured source. The syncword,
     * layer, frequency index, block count, header length and frame length are checked;
     * a single-block CRC is stripped, not checksum-validated.
     */
    fun stripAdtsOrNull(frame: ByteArray): ByteArray? {
        if (frame.size < 7) return null
        val syncword = ((frame[0].toInt() and 0xFF) shl 4) or ((frame[1].toInt() and 0xF0) shr 4)
        if (syncword != 0xFFF) return null
        // ISO/IEC 13818-7: layer is always 00 in ADTS.
        if ((frame[1].toInt() and 0x06) != 0) return null
        val frequencyIndex = (frame[2].toInt() ushr 2) and 0x0f
        if (frequencyIndex >= FREQUENCIES.size) return null
        // number_of_raw_data_blocks_in_frame is the number of blocks minus one.
        if ((frame[6].toInt() and 0x03) != 0) return null
        val protectionAbsent = frame[1].toInt() and 0x01
        val header = if (protectionAbsent == 1) 7 else 9
        if (frame.size <= header) return null
        val declared =
            ((frame[3].toInt() and 0x03) shl 11) or
                ((frame[4].toInt() and 0xFF) shl 3) or
                ((frame[5].toInt() and 0xE0) shr 5)
        if (declared != frame.size) return null
        return frame.copyOfRange(header, frame.size)
    }
}

/** The CMAF sample table's own rules. */
internal object Cmaf {
    /**
     * A sample's sync classification from its trun flags, the way the library
     * reads them: `sample_is_non_sync_sample` (0x00010000) wins, then
     * `sample_depends_on` (bits 24-25), and flags of zero is a sync sample.
     */
    fun sampleIsSync(flags: UInt): Boolean {
        if (flags and 0x00010000u != 0u) return false
        return when ((flags shr 24) and 0x3u) {
            1u -> false
            2u -> true
            else -> flags == 0u
        }
    }
}

/**
 * What the sample will play, on the clock it declares.
 *
 * Under the ruled RAW policy the publisher supplies EXPLICIT epoch capture
 * instants, so the receive side derives ONE broadcast-wide relationship from
 * them: the first RECEIVED instant is the origin, and it never moves. [At] is a
 * SIGNED offset on that relationship -- an earlier capture that arrives later
 * is ordinary interleaving, not a fault -- and it is not yet a decoder input or
 * a render deadline; the platform scheduling policy is separately owed.
 *
 * An object with no capture timestamp is a named refusal for TIMED playback,
 * which is the sample's stricter policy, not a protocol error: the C parser
 * legitimately accepts an untimestamped RAW object.
 */
internal sealed interface PlaybackTiming {
    /** Microseconds from this broadcast's origin; negative is legitimate. */
    data class At(val playbackUs: Long) : PlaybackTiming

    data class Refused(val reason: String) : PlaybackTiming
}

/** The playback clock: one immutable origin for the whole broadcast. */
internal class PlaybackClock {
    private var origin: UnixMicros? = null

    /** The origin, once established. It never moves. */
    val originUs: UnixMicros?
        get() = origin

    fun timeOf(obj: ObjectFacts): PlaybackTiming {
        val capture = obj.captureTime
            ?: return PlaybackTiming.Refused("this object carries no capture timestamp; timed playback needs one")
        val base = origin
        if (base == null) {
            origin = capture
            return PlaybackTiming.At(0L)
        }
        // The DIFFERENCE is what must be representable: two epoch instants may
        // both exceed Long.MAX_VALUE and still be milliseconds apart, so
        // neither absolute value is narrowed first.
        val limit = Long.MAX_VALUE.toULong()
        return if (capture.value >= base.value) {
            val delta = capture.value - base.value
            if (delta > limit) {
                PlaybackTiming.Refused("this capture instant is too far after the origin to place")
            } else {
                PlaybackTiming.At(delta.toLong())
            }
        } else {
            // Backwards, the representable range is one wider: a delta of
            // exactly 2^63 is Long.MIN_VALUE.
            val delta = base.value - capture.value
            when {
                delta == limit + 1uL -> PlaybackTiming.At(Long.MIN_VALUE)
                delta > limit -> PlaybackTiming.Refused("this capture instant is too far before the origin to place")
                else -> PlaybackTiming.At(-delta.toLong())
            }
        }
    }
}

/** The format decision for one track. */
internal object DecoderFormats {
    fun of(facts: TrackFacts): FormatOutcome {
        val codec = facts.codec ?: return FormatOutcome.Unsupported("the track declares no codec")
        val packaging = facts.packaging ?: return FormatOutcome.Unsupported("the track declares no packaging")
        val video = codec.startsWith("avc1.") || codec.startsWith("avc3.")
        val audio = codec.startsWith("mp4a.40.")
        if (!video && !audio) return FormatOutcome.Unsupported("this sample decodes AVC and AAC only, not " + codec)
        if (packaging == PackagingKind.Cmaf && facts.timescale == 0u) {
            return FormatOutcome.Unsupported("a CMAF sample table has no tick rate on this track")
        }
        val config = facts.codecConfig
            ?: return FormatOutcome.Unsupported("the track carries no codec configuration for " + codec)
        val framing =
            if (packaging == PackagingKind.Cmaf) FramingPolicy.StrictLengthPrefixed else FramingPolicy.LocDisambiguated
        if (video) {
            if (facts.mediaType == MediaType.Audio) return FormatOutcome.Unsupported("an AVC codec on an audio track")
            val avc = Avc.parseConfigOrNull(config)
                ?: return FormatOutcome.Unsupported("the AVC configuration record is malformed")
            // MediaCodec takes AVC csd-0 as the SPS and csd-1 as the PPS, each
            // start-code framed; every parameter set the record carried is kept,
            // in record order.
            if (facts.width == 0u || facts.width > Int.MAX_VALUE.toUInt() ||
                facts.height == 0u || facts.height > Int.MAX_VALUE.toUInt()
            ) {
                return FormatOutcome.Unsupported(
                    "a video format needs positive dimensions this platform can carry; got " +
                        facts.width + "x" + facts.height,
                )
            }
            fun framed(sets: List<ByteArray>): ByteArray {
                val out = ArrayList<Byte>()
                for (set in sets) {
                    for (b in MediaContract.START_CODE) out.add(b)
                    for (b in set) out.add(b)
                }
                return out.toByteArray()
            }
            return FormatOutcome.Video(
                mime = "video/avc",
                width = facts.width.toInt(),
                height = facts.height.toInt(),
                csd = listOf(framed(avc.sequenceParameterSets), framed(avc.pictureParameterSets)),
                avc = avc,
                packaging = packaging,
                framing = framing,
                timescale = facts.timescale,
            )
        }
        if (facts.mediaType == MediaType.Video) return FormatOutcome.Unsupported("an AAC codec on a video track")
        val aac =
            when (val parsed = Aac.parse(config)) {
                is AacParse.Ok -> parsed.config
                is AacParse.No -> return FormatOutcome.Unsupported(parsed.reason)
            }
        return FormatOutcome.Audio(
            mime = "audio/mp4a-latm",
            csd = listOf(config.copyOf()),
            aac = aac,
            packaging = packaging,
            bitstream = facts.aacBitstream,
            timescale = facts.timescale,
        )
    }
}

/**
 * Microsecond conversion for a tick count, CHECKED: the multiplication is
 * refused before it can overflow, and a zero tick rate has no conversion.
 */
private fun ticksToUsOrNull(ticks: Long, timescale: UInt): Long? {
    if (timescale == 0u) return null
    // Quotient and remainder, so a tick count whose ANSWER is representable is
    // converted even when ticks x 1,000,000 would not be. Kotlin's division
    // truncates toward zero, and so does this composition.
    val rate = timescale.toLong()
    val whole = ticks / rate
    val rest = ticks % rate
    if (whole > Long.MAX_VALUE / 1_000_000L || whole < Long.MIN_VALUE / 1_000_000L) return null
    val scaled = whole * 1_000_000L
    // |rest| < rate <= UInt.MAX, so rest * 1,000,000 cannot overflow.
    return addOrNull(scaled, rest * 1_000_000L / rate)
}

private fun addOrNull(a: Long, b: Long): Long? {
    val sum = a + b
    if (((a xor sum) and (b xor sum)) < 0) return null
    return sum
}

/** The access-unit decision for one received object. */
internal object AccessUnits {
    /**
     * The single access unit this object carries. Valid only when the object
     * declares at most one sample: a multi-sample fragment's mdat is a
     * concatenation, and queueing it whole would feed the decoder several
     * frames under one timestamp.
     */
    fun of(format: FormatOutcome, obj: ObjectFacts, maxOutputBytes: Int = Int.MAX_VALUE): AccessUnitOutcome {
        if (obj.status != ObjectStatus.Normal) return AccessUnitOutcome.StatusOnly
        if (obj.samples.size > 1) {
            return AccessUnitOutcome.Rejected(
                "this object declares " + obj.samples.size + " samples; it is not one access unit",
            )
        }
        val sample = obj.samples.firstOrNull()
        val base = obj.decodeTime.value
        if (base > Long.MAX_VALUE.toULong()) return AccessUnitOutcome.Rejected("the decode time has no signed representation")
        // The access unit is the DECLARED sample span. Native C allows trailing
        // mdat bytes (`media_object.c:265-278`), so the whole payload is not
        // the sample.
        val span: ByteArray =
            if (sample == null) {
                obj.payload
            } else {
                if (sample.sizeBytes > Int.MAX_VALUE.toUInt()) {
                    return AccessUnitOutcome.Rejected("a sample size of " + sample.sizeBytes + " bytes is not addressable")
                }
                val size = sample.sizeBytes.toInt()
                if (size <= 0 || size.toLong() > obj.payload.size.toLong()) {
                    return AccessUnitOutcome.Rejected("the declared sample runs past the media bytes")
                }
                obj.payload.copyOfRange(0, size)
            }
        val presentation: Long =
            if (sample == null) {
                val pts = obj.presentationTime.value
                if (pts > Long.MAX_VALUE.toULong()) {
                    return AccessUnitOutcome.Rejected("the presentation time has no signed representation")
                }
                pts.toLong()
            } else {
                val timescale = timescaleOf(format)
                val offset = ticksToUsOrNull(sample.compositionOffsetTicks.toLong(), timescale)
                    ?: return AccessUnitOutcome.Rejected("this sample's composition offset does not convert")
                addOrNull(base.toLong(), offset)
                    ?: return AccessUnitOutcome.Rejected("this sample's presentation time does not fit")
            }
        val bytes = decode(format, span, maxOutputBytes)
            ?: return AccessUnitOutcome.Rejected("the payload does not hold the framing this track declares")
        // The FIRST sample keeps the object's own keyframe fact: the library
        // falls back to the sample flags only when no explicit video frame
        // marking decided it (`media_object.c:204-214,327-331`).
        return AccessUnitOutcome.Unit(bytes, presentation, obj.keyframe)
    }

    /**
     * Every access unit in the object, split by its declared sample table, in
     * order. The whole object is refused if any sample is invalid: a partial
     * prefix must never reach the decoder.
     */
    fun split(format: FormatOutcome, obj: ObjectFacts, maxOutputBytes: Int = Int.MAX_VALUE): List<AccessUnitOutcome> {
        if (obj.status != ObjectStatus.Normal) return listOf(AccessUnitOutcome.StatusOnly)
        if (obj.samples.isEmpty()) return listOf(of(format, obj, maxOutputBytes))
        val timescale = timescaleOf(format)
        val base = obj.decodeTime.value
        if (base > Long.MAX_VALUE.toULong()) {
            return listOf(AccessUnitOutcome.Rejected("the decode time has no signed representation"))
        }
        val units = ArrayList<AccessUnitOutcome>(obj.samples.size)
        var at = 0
        var elapsedTicks = 0L
        var remaining = maxOutputBytes
        for (sample in obj.samples) {
            if (sample.sizeBytes > Int.MAX_VALUE.toUInt()) {
                return listOf(AccessUnitOutcome.Rejected("a sample size of " + sample.sizeBytes + " bytes is not addressable"))
            }
            val size = sample.sizeBytes.toInt()
            // Long arithmetic, so the bound is decided before any slice is taken.
            if (size <= 0 || at.toLong() + size.toLong() > obj.payload.size.toLong()) {
                return listOf(AccessUnitOutcome.Rejected("sample " + units.size + " runs past the media bytes"))
            }
            // CUMULATIVE: the elapsed ticks are converted once, so the answer
            // never drifts by a truncation per sample.
            val elapsed = ticksToUsOrNull(elapsedTicks, timescale)
                ?: return listOf(AccessUnitOutcome.Rejected("the elapsed tick count does not convert"))
            val decode = addOrNull(base.toLong(), elapsed)
                ?: return listOf(AccessUnitOutcome.Rejected("sample " + units.size + " has no representable decode time"))
            val offset = ticksToUsOrNull(sample.compositionOffsetTicks.toLong(), timescale)
                ?: return listOf(AccessUnitOutcome.Rejected("sample " + units.size + " has no representable composition offset"))
            val presentation = addOrNull(decode, offset)
                ?: return listOf(AccessUnitOutcome.Rejected("sample " + units.size + " has no representable presentation time"))
            val bytes = decode(format, obj.payload.copyOfRange(at, at + size), remaining)
                ?: return listOf(AccessUnitOutcome.Rejected("sample " + units.size + " does not hold the declared framing"))
            remaining -= bytes.size
            // The first sample carries the object's own keyframe fact; later
            // samples use their own flags.
            val sync = if (units.isEmpty()) obj.keyframe else Cmaf.sampleIsSync(sample.flags)
            units.add(AccessUnitOutcome.Unit(bytes, presentation, sync))
            at += size
            if (sample.durationTicks > Long.MAX_VALUE.toULong()) {
                return listOf(AccessUnitOutcome.Rejected("a sample duration that large has no representation"))
            }
            elapsedTicks = addOrNull(elapsedTicks, sample.durationTicks.toLong())
                ?: return listOf(AccessUnitOutcome.Rejected("the elapsed tick count overflows"))
        }
        return units
    }

    private fun timescaleOf(format: FormatOutcome): UInt =
        when (format) {
            is FormatOutcome.Video -> format.timescale
            is FormatOutcome.Audio -> format.timescale
            is FormatOutcome.Unsupported -> 0u
        }

    /** The decoder-ready bytes for one access unit, or `null` when refused. */
    private fun decode(format: FormatOutcome, bytes: ByteArray, maxOutputBytes: Int): ByteArray? =
        if (maxOutputBytes <= 0) null else when (format) {
            is FormatOutcome.Video ->
                when (format.framing) {
                    FramingPolicy.StrictLengthPrefixed -> Avc.toAnnexBOrNull(bytes, format.avc, maxOutputBytes)
                    FramingPolicy.LocDisambiguated ->
                        when (Avc.framingOf(bytes, format.avc)) {
                            VideoFraming.AnnexB -> if (bytes.size > maxOutputBytes) null else Avc.copyAnnexBOrNull(bytes)
                            VideoFraming.LengthPrefixed -> Avc.toAnnexBOrNull(bytes, format.avc, maxOutputBytes)
                        }
                }
            is FormatOutcome.Audio ->
                if (bytes.size > maxOutputBytes) null else when (format.bitstream) {
                    AacBitstream.Raw -> bytes.copyOf()
                    AacBitstream.Adts -> Aac.stripAdtsOrNull(bytes)
                }
            is FormatOutcome.Unsupported -> null
        }
}
