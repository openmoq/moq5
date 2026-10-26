package org.openmoq.moq5.samples.publisher

/**
 * What the encoder SAID it was configured for: the sample rate and channel
 * count from its output format. They are compared with the configuration;
 * they never stand in for it.
 */
internal class DeclaredAudioFacts(val sampleRate: Int, val channelCount: Int) {
    init {
        require(sampleRate > 0) { "a declared sample rate is positive, not $sampleRate" }
        require(channelCount > 0) { "a declared channel count is positive, not $channelCount" }
    }
}

/**
 * The parsed core facts of an AudioSpecificConfig inside this sample's
 * supported subset: AAC-LC, an indexed sampling frequency, one or two
 * channels, 1024-sample access units. They describe the configuration, never
 * what the encoder was asked for. The type does not prove agreement with the
 * encoder: a [AacConfigVerdict.Mismatched] carries these facts too. Admission
 * and keying take facts ONLY from [AacConfigVerdict.Supported].
 */
internal class AacLcConfigFacts internal constructor(
    val sampleRate: Int,
    val channelCount: Int,
    /** Samples per channel in one access unit, from frameLengthFlag. Never inferred from the object type or a codec string. */
    val framesPerAccessUnit: Int,
)

/** An AudioSpecificConfig field, named where the bytes end before it. */
internal enum class AscField {
    AUDIO_OBJECT_TYPE,
    SAMPLING_FREQUENCY_INDEX,
    SAMPLING_FREQUENCY,
    CHANNEL_CONFIGURATION,
    FRAME_LENGTH_FLAG,
    DEPENDS_ON_CORE_CODER,
    EXTENSION_FLAG,
    SYNC_EXTENSION_TYPE,
    EXTENSION_AUDIO_OBJECT_TYPE,
    SBR_PRESENT_FLAG,
    EXTENSION_SAMPLING_FREQUENCY_INDEX,
    EXTENSION_SAMPLING_FREQUENCY,
}

/**
 * Syntax outside this sample's supported subset, found along the path that
 * was actually parsed. It certifies nothing about the rest: where a feature
 * stops the examination, the validity of the unexamined continuation is
 * UNKNOWN, and such a configuration is refused, not judged well formed.
 */
internal sealed interface AscFeature {
    val description: String

    /** The configuration arrived as other than exactly one unit. */
    data class UnitCount(val count: Int) : AscFeature {
        override val description: String get() = "$count configuration units; the sample reads exactly one AudioSpecificConfig"
    }

    /** Any object type but AAC LC (2), including explicit SBR (5) and the prefix 31. Examination stops. */
    data class AudioObjectType(val type: Int) : AscFeature {
        override val description: String get() = "audioObjectType $type; the sample supports AAC LC (2) only"
    }

    /** A samplingFrequencyIndex outside the sample's indexed table (13 or 14). */
    data class FrequencyIndexOutsideSubset(val index: Int) : AscFeature {
        override val description: String get() = "samplingFrequencyIndex $index is outside the sample's verified set"
    }

    /** samplingFrequencyIndex 0xf with its 24-bit samplingFrequency. */
    data class ExplicitFrequency(val hertz: Int) : AscFeature {
        override val description: String get() = "an explicit samplingFrequency of $hertz Hz; the sample supports indexed frequencies only"
    }

    /** channelConfiguration 0: the layout is in a program_config_element. Examination stops. */
    data object ProgramConfigElement : AscFeature {
        override val description: String get() = "channelConfiguration 0 (a program_config_element layout); the sample supports 1 or 2"
    }

    /** Any other channelConfiguration but 1 or 2. Examination stops above 7. */
    data class ChannelConfiguration(val code: Int) : AscFeature {
        override val description: String get() = "channelConfiguration $code; the sample supports 1 or 2"
    }

    /** frameLengthFlag 1: not the 1024-sample access unit this sample keys. */
    data object FrameLengthFlagSet : AscFeature {
        override val description: String get() = "frameLengthFlag 1 (not the 1024-sample access unit)"
    }

    /** dependsOnCoreCoder 1. Examination stops. */
    data object CoreCoderDependency : AscFeature {
        override val description: String get() = "dependsOnCoreCoder 1; the sample supports no core coder"
    }

    /** The GASpecificConfig extensionFlag is set. Examination stops. */
    data object GaExtensionFlag : AscFeature {
        override val description: String get() = "the GASpecificConfig extensionFlag is set"
    }

    /** A syncExtensionType 0x2b7 extension; [sbrPresent] is null when the extension object type is not 5. */
    data class SyncExtension(val extensionAudioObjectType: Int, val sbrPresent: Boolean?) : AscFeature {
        override val description: String get() =
            "a backward-compatible extension (0x2b7) naming object type $extensionAudioObjectType" +
                (sbrPresent?.let { ", sbrPresentFlag ${if (it) 1 else 0}" } ?: "")
    }

    /** Bits after the configuration that are not a 0x2b7 extension. Refusing them is this sample's subset policy, not a claim that padding is malformed. */
    data class TrailingData(val bits: Long) : AscFeature {
        override val description: String get() = "$bits bit(s) after the configuration that are not a 0x2b7 extension"
    }
}

internal sealed interface AacConfigVerdict {
    /** The configuration is inside the supported subset and agrees with the declared facts. */
    class Supported(val facts: AacLcConfigFacts) : AacConfigVerdict

    /** The bytes end before [field], which [bitsNeeded] bits would have held; only [bitsAvailable] exist. */
    class Malformed(val field: AscField, val bitsAvailable: Long, val bitsNeeded: Long) : AacConfigVerdict {
        val reason: String
            get() = "the configuration ends at bit $bitsAvailable, before its ${this.field} (which needs $bitsNeeded)"
    }

    /** Outside the supported subset; every feature found along the parsed path, in order. Nothing past a stop point was examined. */
    class Unsupported(val features: List<AscFeature>) : AacConfigVerdict {
        val reason: String get() = features.joinToString("; ") { it.description }
    }

    /** A supported configuration whose actual facts disagree with what the encoder declared. */
    class Mismatched(val actual: AacLcConfigFacts, val declared: DeclaredAudioFacts) : AacConfigVerdict {
        val sampleRateDiffers: Boolean get() = actual.sampleRate != declared.sampleRate
        val channelCountDiffers: Boolean get() = actual.channelCount != declared.channelCount
    }
}

/**
 * The sample's AAC configuration semantics: synchronous, bounded by the
 * snapshot it reads, and codec-specific only here. It reads the OWNED raw
 * snapshot through its copying accessor and never keeps the copy. It does not
 * catch: an allocation failure or any other Throwable leaves as itself.
 */
internal object AacConfigSemantics {
    /**
     * samplingFrequencyIndex 0..12 in Hz. Evidence, not ISO text: the
     * Android/Fraunhofer decoder table (external/aac tp_data.h at
     * 4803bf75f564e5c9304527a5b902711f78e9f621, SamplingRateTable). 13 and 14
     * are outside this table; 15 is the explicit-frequency escape.
     */
    val VERIFIED_FREQUENCY_INDICES: Map<Int, Int> =
        listOf(96_000, 88_200, 64_000, 48_000, 44_100, 32_000, 24_000, 22_050, 16_000, 12_000, 11_025, 8_000, 7_350)
            .withIndex().associate { it.index to it.value }

    private const val SYNC_EXTENSION_TYPE = 0x2b7L

    /**
     * The fields of the one configuration unit, in order: audioObjectType,
     * samplingFrequencyIndex (and a 24-bit samplingFrequency after the escape
     * 0xf), channelConfiguration, then the GASpecificConfig flags
     * frameLengthFlag, dependsOnCoreCoder and extensionFlag, then anything
     * after them. A shortage on the parsed path is [AacConfigVerdict.Malformed];
     * a value outside the subset is recorded and, where the sample does not
     * parse what it implies, ends the examination there. Nothing is Supported
     * until every field AND the absence of trailing bits has been checked.
     */
    fun examine(config: RawConfigSnapshot, declared: DeclaredAudioFacts): AacConfigVerdict {
        if (config.unitCount != 1) return AacConfigVerdict.Unsupported(listOf(AscFeature.UnitCount(config.unitCount)))
        val r = BitReader(config.unit(0))
        val found = ArrayList<AscFeature>()
        fun stop(feature: AscFeature) = AacConfigVerdict.Unsupported(found + feature)

        r.shortage(AscField.AUDIO_OBJECT_TYPE, 5)?.let { return it }
        val objectType = r.read(5).toInt()
        if (objectType != 2) return stop(AscFeature.AudioObjectType(objectType))

        r.shortage(AscField.SAMPLING_FREQUENCY_INDEX, 4)?.let { return it }
        val index = r.read(4).toInt()
        var sampleRate = 0
        if (index == 0xf) {
            r.shortage(AscField.SAMPLING_FREQUENCY, 24)?.let { return it }
            found += AscFeature.ExplicitFrequency(r.read(24).toInt())
        } else {
            val rate = VERIFIED_FREQUENCY_INDICES[index]
            if (rate == null) found += AscFeature.FrequencyIndexOutsideSubset(index) else sampleRate = rate
        }

        r.shortage(AscField.CHANNEL_CONFIGURATION, 4)?.let { return it }
        val channels = r.read(4).toInt()
        when (channels) {
            1, 2 -> Unit
            0 -> return stop(AscFeature.ProgramConfigElement)
            in 3..7 -> found += AscFeature.ChannelConfiguration(channels)
            else -> return stop(AscFeature.ChannelConfiguration(channels))
        }

        r.shortage(AscField.FRAME_LENGTH_FLAG, 1)?.let { return it }
        if (r.read(1) == 1L) found += AscFeature.FrameLengthFlagSet
        r.shortage(AscField.DEPENDS_ON_CORE_CODER, 1)?.let { return it }
        if (r.read(1) == 1L) return stop(AscFeature.CoreCoderDependency)
        r.shortage(AscField.EXTENSION_FLAG, 1)?.let { return it }
        if (r.read(1) == 1L) return stop(AscFeature.GaExtensionFlag)

        // After the configuration: a 0x2b7 extension is read only when at
        // least 16 bits remain; anything else there is trailing data. Both are
        // outside this sample's subset.
        val trailing = r.remaining
        if (trailing >= 16) {
            if (r.read(11) == SYNC_EXTENSION_TYPE) {
                r.shortage(AscField.EXTENSION_AUDIO_OBJECT_TYPE, 5)?.let { return it }
                val extensionType = r.read(5).toInt()
                if (extensionType == 5) {
                    r.shortage(AscField.SBR_PRESENT_FLAG, 1)?.let { return it }
                    val sbrPresent = r.read(1) == 1L
                    if (sbrPresent) {
                        r.shortage(AscField.EXTENSION_SAMPLING_FREQUENCY_INDEX, 4)?.let { return it }
                        if (r.read(4) == 0xfL) {
                            r.shortage(AscField.EXTENSION_SAMPLING_FREQUENCY, 24)?.let { return it }
                            r.read(24)
                        }
                    }
                    found += AscFeature.SyncExtension(5, sbrPresent)
                } else {
                    found += AscFeature.SyncExtension(extensionType, null)
                }
            } else {
                found += AscFeature.TrailingData(trailing)
            }
        } else if (trailing > 0) {
            found += AscFeature.TrailingData(trailing)
        }

        if (found.isNotEmpty()) return AacConfigVerdict.Unsupported(found)
        val facts = AacLcConfigFacts(sampleRate, channels, AAC_LC_FRAMES_PER_ACCESS_UNIT)
        return if (facts.sampleRate == declared.sampleRate && facts.channelCount == declared.channelCount) {
            AacConfigVerdict.Supported(facts)
        } else {
            AacConfigVerdict.Mismatched(facts, declared)
        }
    }

    /** The audio key authority for a SUPPORTED configuration's facts: their rate and access-unit length, from [originUs]. */
    fun keyingFor(facts: AacLcConfigFacts, originUs: Long): UnitAudioKeying =
        UnitAudioKeying(facts.sampleRate, facts.framesPerAccessUnit, originUs)

    /** frameLengthFlag 0: the access-unit length this sample supports. */
    private const val AAC_LC_FRAMES_PER_ACCESS_UNIT = 1024

    /** Most-significant-bit-first reads over one owned copy. Positions and counts are Long: a unit's bit count can exceed Int. */
    private class BitReader(private val bytes: ByteArray) {
        private val total = bytes.size.toLong() * 8
        private var position = 0L

        val remaining: Long get() = total - position

        /** The Malformed verdict when fewer than [width] bits remain for [field], else null. */
        fun shortage(field: AscField, width: Int): AacConfigVerdict.Malformed? =
            if (remaining < width) AacConfigVerdict.Malformed(field, total, position + width) else null

        fun read(width: Int): Long {
            var value = 0L
            repeat(width) {
                val bit = (bytes[(position ushr 3).toInt()].toInt() ushr (7 - (position and 7L).toInt())) and 1
                value = (value shl 1) or bit.toLong()
                position++
            }
            return value
        }
    }
}
