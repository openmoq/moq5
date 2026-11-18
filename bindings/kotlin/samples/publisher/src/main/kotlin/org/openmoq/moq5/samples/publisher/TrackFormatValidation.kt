package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecSignaling
import org.openmoq.moq5.CodecSignalingException
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.SampleEntry
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.TrackName

/**
 * What the video encoder's output format declared, as the driver read it:
 * each fact is absent (null) when the format did not carry it. Width and
 * height are declared encoder facts, never parsed from an SPS.
 *
 * [maximumBitrate] is the track's declared MAXIMUM in bits per second (MSF
 * 5.2.22), supplied independently of the encoder's configured target. It is
 * what the catalog carries; declaring it creates no platform guarantee that
 * the encoder respects it. [targetBitrate] is diagnostic only: it never
 * satisfies, replaces or bounds the maximum, and it does not enter the
 * validated configuration.
 */
internal class VideoFormatObservation(
    val width: Int?,
    val height: Int?,
    val targetBitrate: Int?,
    val maximumBitrate: ULong?,
    /** The format's codec-specific data, csd-0 then csd-1, borrowed for the synchronous validation only. */
    val config: List<ByteBuffer>,
)

/** The audio counterpart: declared sample rate and channel count, the declared maximum and diagnostic target, and csd-0. */
internal class AudioFormatObservation(
    val sampleRate: Int?,
    val channelCount: Int?,
    val targetBitrate: Int?,
    val maximumBitrate: ULong?,
    val config: List<ByteBuffer>,
)

/**
 * The codec string of a SUPPORTED AudioSpecificConfig, through the public
 * utility, with a refusal of the input (and only that) as an outcome.
 */
internal interface AacStringUtility {
    fun formatCodecString(audioSpecificConfig: MediaBytes): UtilityOutcome<String>
}

/**
 * The production AAC string utility: `mp4a` over the AudioSpecificConfig
 * with object type indication 0x40 (MPEG-4 audio), through the public
 * binding. Only [CodecSignalingException] becomes an outcome.
 */
internal object PublicAacSignaling : AacStringUtility {
    const val MPEG4_AUDIO_OBJECT_TYPE_INDICATION = 0x40

    val MP4A: SampleEntry = SampleEntry("mp4a")

    override fun formatCodecString(audioSpecificConfig: MediaBytes): UtilityOutcome<String> =
        try {
            UtilityOutcome.Done(
                CodecSignaling.formatCodecString(
                    CodecConfigFormat.AacAudioSpecificConfig,
                    MP4A,
                    audioSpecificConfig,
                    MPEG4_AUDIO_OBJECT_TYPE_INDICATION,
                ),
            )
        } catch (e: CodecSignalingException) {
            UtilityOutcome.Refused(e.code)
        }
}

/**
 * The immutable comparison authority a complete format was validated with,
 * carried beside the track: the resolved AVC configuration (its record,
 * string and parameter-set inventory) or the supported AAC facts that key
 * the audio track. Never reconstructed from catalog state.
 */
internal sealed interface FormatAuthority {
    val kind: MediaKind

    class Avc(val config: AvcDecoderConfig) : FormatAuthority {
        override val kind: MediaKind get() = MediaKind.Video
    }

    class Aac(val facts: AacLcConfigFacts) : FormatAuthority {
        override val kind: MediaKind get() = MediaKind.Audio
    }
}

/** Why an observed output format did not become a complete track format. */
internal sealed interface TrackFormatRefusal {
    val description: String

    /** A fact the track needs was absent from the observation. */
    data class MissingFact(val fact: String) : TrackFormatRefusal {
        override val description: String get() = "the output format declared no $fact"
    }

    /**
     * The output format's MIME type is not the one this track's encoder was
     * configured for, or it declared none ([actual] null). Nothing else was
     * read, so no configuration reaches another codec's validator.
     */
    data class Mime(val expected: String, val actual: String?) : TrackFormatRefusal {
        override val description: String
            get() = if (actual == null) "the output format declared no MIME type; the track is $expected" else "the output format's MIME type is $actual, not $expected"
    }

    /** A declared fact is not positive. */
    data class InvalidFact(val fact: String, val value: Int) : TrackFormatRefusal {
        override val description: String get() = "the output format declared $fact $value"
    }

    /** A declared maximum bitrate of zero: MSF 5.2.22 requires one for audio and video. */
    data object ZeroMaximumBitrate : TrackFormatRefusal {
        override val description: String get() = "the declared maximum bitrate is 0"
    }

    /** The configuration could not be captured within its bounds. */
    data class Snapshot(val reason: String) : TrackFormatRefusal {
        override val description: String get() = "the configuration was not captured: $reason"
    }

    /**
     * The AVC configuration step did not resolve: any verdict but Resolved,
     * kept structured. The description is built from the verdict's own typed
     * detail: the defect, the subset refusal, or the utility stage and code.
     */
    class Avc(val verdict: AvcConfigVerdict) : TrackFormatRefusal {
        init {
            require(verdict !is AvcConfigVerdict.Resolved) { "a resolved AVC configuration is not a refusal" }
        }

        override val description: String
            get() =
                when (verdict) {
                    is AvcConfigVerdict.Malformed -> "the AVC configuration is malformed: ${verdict.defect.description}"
                    is AvcConfigVerdict.OutsideSubset -> "the AVC configuration is outside the sample's subset: ${verdict.refusal.description}"
                    is AvcConfigVerdict.UtilityRefused ->
                        "the codec utility refused the AVC configuration at ${verdict.stage.name} with code ${verdict.code.value}"
                    is AvcConfigVerdict.Resolved -> error("a resolved AVC configuration is not a refusal")
                }
    }

    /**
     * The AAC semantics did not find a supported, agreeing configuration: any
     * verdict but Supported, kept structured. The description is built from
     * the verdict's own detail: the shortage, the features found, or the
     * actual and declared facts.
     */
    class Aac(val verdict: AacConfigVerdict) : TrackFormatRefusal {
        init {
            require(verdict !is AacConfigVerdict.Supported) { "a supported AAC configuration is not a refusal" }
        }

        override val description: String
            get() =
                when (verdict) {
                    is AacConfigVerdict.Malformed -> "the AAC configuration is malformed: ${verdict.reason}"
                    is AacConfigVerdict.Unsupported -> "the AAC configuration is outside the sample's subset: ${verdict.reason}"
                    is AacConfigVerdict.Mismatched ->
                        "the AAC configuration says ${verdict.actual.sampleRate} Hz and ${verdict.actual.channelCount} channel(s); " +
                            "the encoder declared ${verdict.declared.sampleRate} Hz and ${verdict.declared.channelCount} channel(s)"
                    is AacConfigVerdict.Supported -> error("a supported AAC configuration is not a refusal")
                }
    }

    /** The public utility refused the supported AudioSpecificConfig's codec string. */
    data class AacString(val code: MoqResultCode) : TrackFormatRefusal {
        override val description: String get() = "the codec utility refused the AAC codec string with code ${code.value}"
    }
}

internal sealed interface TrackFormatVerdict {
    class Complete(val format: CompleteTrackFormat) : TrackFormatVerdict

    class Refused(val kind: MediaKind, val refusal: TrackFormatRefusal) : TrackFormatVerdict
}

/** A track's format slot failed with [refusal]: the source's own failure, which the format wait keeps by identity. */
internal class TrackFormatRefused(val kind: MediaKind, val refusal: TrackFormatRefusal) :
    IllegalStateException("the $kind format was refused: ${refusal.description}")

/**
 * The validator: one observed output format to a complete track format, or
 * a named refusal. Declared facts are checked first -- video width, height,
 * maximum bitrate; audio sample rate, channel count, maximum bitrate -- so a
 * partial observation reads no configuration unit, copies nothing and calls
 * no utility. The diagnostic target bitrate is never checked or carried.
 * The declared maximum becomes the configuration's bitrate unchanged, over
 * its whole unsigned range. Then the configuration is
 * captured within [ConfigBounds] and validated by the accepted AVC or AAC
 * step, unchanged. Synchronous and catch-free.
 */
internal object TrackFormats {
    fun video(
        name: TrackName,
        observed: VideoFormatObservation,
        bounds: ConfigBounds,
        utility: AvcRecordUtility = PublicCodecSignaling,
    ): TrackFormatVerdict = CompleteTrackFormat.video(name, observed, bounds, utility)

    fun audio(
        name: TrackName,
        observed: AudioFormatObservation,
        bounds: ConfigBounds,
        utility: AacStringUtility = PublicAacSignaling,
    ): TrackFormatVerdict = CompleteTrackFormat.audio(name, observed, bounds, utility)
}

/**
 * A COMPLETE, validated configuration for one track, together with the
 * comparison [authority] it was validated with. Construction is private to
 * this class: the only way to make one is the validating factories below,
 * reached through [TrackFormats], so a configuration and an authority that
 * disagree -- a catalog record that is not the one the authority retained --
 * cannot be paired. A partial observation -- a codec-config buffer, a single
 * `MediaFormat` field, a sample rate seen before its channel layout -- is
 * never one of these. The value is immutable: [SendTrackConfig] carries
 * value bytes only, and the authority is owned.
 */
internal class CompleteTrackFormat private constructor(val config: SendTrackConfig, val authority: FormatAuthority) {
    val kind: MediaKind get() = authority.kind

    init {
        val expected = if (authority.kind == MediaKind.Video) MediaType.Video else MediaType.Audio
        require(config.mediaType == expected) { "a ${authority.kind} format carries a ${config.mediaType} configuration" }
    }

    /** The validating factories: the algorithm [TrackFormats] documents, and the only callers of the constructor. */
    internal companion object {
        fun video(
            name: TrackName,
            observed: VideoFormatObservation,
            bounds: ConfigBounds,
            utility: AvcRecordUtility = PublicCodecSignaling,
        ): TrackFormatVerdict {
            fun refused(refusal: TrackFormatRefusal) = TrackFormatVerdict.Refused(MediaKind.Video, refusal)
            val width = observed.width ?: return refused(TrackFormatRefusal.MissingFact("width"))
            if (width <= 0) return refused(TrackFormatRefusal.InvalidFact("width", width))
            val height = observed.height ?: return refused(TrackFormatRefusal.MissingFact("height"))
            if (height <= 0) return refused(TrackFormatRefusal.InvalidFact("height", height))
            val maximum = observed.maximumBitrate ?: return refused(TrackFormatRefusal.MissingFact("maximum bitrate"))
            if (maximum == 0uL) return refused(TrackFormatRefusal.ZeroMaximumBitrate)

            val snapshot =
                when (val taken = RawConfigSnapshots.capture(observed.config, bounds)) {
                    is RawSnapshotResult.Refused -> return refused(TrackFormatRefusal.Snapshot(taken.reason))
                    is RawSnapshotResult.Taken -> taken.snapshot
                }
            val resolved =
                when (val verdict = AvcConfiguration.resolve(snapshot, utility)) {
                    is AvcConfigVerdict.Resolved -> verdict.config
                    else -> return refused(TrackFormatRefusal.Avc(verdict))
                }
            val config =
                SendTrackConfig(
                    name = name,
                    mediaType = MediaType.Video,
                    packaging = PackagingKind.Raw,
                    codec = MediaBytes(resolved.codecString.encodeToByteArray()),
                    bitrate = maximum,
                    initData = resolved.record,
                    width = width.toUInt(),
                    height = height.toUInt(),
                )
            return TrackFormatVerdict.Complete(CompleteTrackFormat(config, FormatAuthority.Avc(resolved)))
        }

        fun audio(
            name: TrackName,
            observed: AudioFormatObservation,
            bounds: ConfigBounds,
            utility: AacStringUtility = PublicAacSignaling,
        ): TrackFormatVerdict {
            fun refused(refusal: TrackFormatRefusal) = TrackFormatVerdict.Refused(MediaKind.Audio, refusal)
            val sampleRate = observed.sampleRate ?: return refused(TrackFormatRefusal.MissingFact("sample rate"))
            if (sampleRate <= 0) return refused(TrackFormatRefusal.InvalidFact("sample rate", sampleRate))
            val channelCount = observed.channelCount ?: return refused(TrackFormatRefusal.MissingFact("channel count"))
            if (channelCount <= 0) return refused(TrackFormatRefusal.InvalidFact("channel count", channelCount))
            val maximum = observed.maximumBitrate ?: return refused(TrackFormatRefusal.MissingFact("maximum bitrate"))
            if (maximum == 0uL) return refused(TrackFormatRefusal.ZeroMaximumBitrate)

            val snapshot =
                when (val taken = RawConfigSnapshots.capture(observed.config, bounds)) {
                    is RawSnapshotResult.Refused -> return refused(TrackFormatRefusal.Snapshot(taken.reason))
                    is RawSnapshotResult.Taken -> taken.snapshot
                }
            val facts =
                when (val verdict = AacConfigSemantics.examine(snapshot, DeclaredAudioFacts(sampleRate, channelCount))) {
                    is AacConfigVerdict.Supported -> verdict.facts
                    else -> return refused(TrackFormatRefusal.Aac(verdict))
                }
            // The supported configuration is exactly one unit: it is the catalog's
            // initData and what the codec string is formatted from.
            val audioSpecificConfig = MediaBytes(snapshot.unit(0))
            val codecString =
                when (val formatted = utility.formatCodecString(audioSpecificConfig)) {
                    is UtilityOutcome.Refused -> return refused(TrackFormatRefusal.AacString(formatted.code))
                    is UtilityOutcome.Done -> formatted.value
                }
            val config =
                SendTrackConfig(
                    name = name,
                    mediaType = MediaType.Audio,
                    packaging = PackagingKind.Raw,
                    codec = MediaBytes(codecString.encodeToByteArray()),
                    bitrate = maximum,
                    initData = audioSpecificConfig,
                    samplerate = facts.sampleRate.toUInt(),
                    channelConfig = MediaBytes(facts.channelCount.toString().encodeToByteArray()),
                )
            return TrackFormatVerdict.Complete(CompleteTrackFormat(config, FormatAuthority.Aac(facts)))
        }
    }
}

/** What offering one validator verdict to a slot did. */
internal sealed interface SlotOffer {
    /** The first verdict was complete: the slot now holds [format]. */
    class Resolved(val format: CompleteTrackFormat) : SlotOffer

    /** The first verdict was a refusal: the slot failed with [failure]. */
    class Refused(val failure: TrackFormatRefused) : SlotOffer

    /** A later complete verdict whose configuration equals the held one: no effect. */
    data object Equal : SlotOffer

    /** A later verdict that is refused, or complete with a different configuration. */
    class Changed(val held: CompleteTrackFormat, val observed: TrackFormatVerdict) : SlotOffer

    /** The slot already failed: no later verdict repairs it. */
    data object AlreadyRefused : SlotOffer

    /** The slot failed with an unexpected [cause] through [TrackFormatSlot.fail]: no later verdict repairs it. */
    class AlreadyFailed(val cause: Throwable) : SlotOffer
}

/**
 * One track's format slot: the only producer of that track's
 * [FormatSources] entry. The first selection settles it -- a complete
 * verdict, a typed refusal failing it with a [TrackFormatRefused], or an
 * unexpected failure through [fail] failing it with that exact cause -- and
 * nothing afterwards changes the settled value; a later verdict is
 * classified, never adopted. Equality is the full
 * validated [org.openmoq.moq5.SendTrackConfig], whose initData is the exact
 * record or AudioSpecificConfig. A verdict of another kind is a contract
 * failure that changes nothing.
 *
 * **State selection and delivery are two events.** Any thread may offer,
 * concurrently or reentrantly. Each [offer] runs ONE short critical section
 * under the slot's lock: it checks the kind, then either records the first
 * verdict as the held state or classifies against the state already held.
 * The first verdict's linearization point is that record. Only the offer
 * that recorded it then completes the deferred -- AFTER releasing the lock,
 * because completion runs external handlers and resumes awaiters, and a
 * handler that waits on another thread's offer to this slot must not be
 * holding it up. A later offer, from a handler inside that completion or
 * from another thread, classifies against the recorded state without
 * waiting for the delivery. Such a classification is not evidence that the
 * delivery finished or that any waiter ran, and it starts nothing: the pair
 * wait still observes only the deferred, completed exactly once.
 */
internal class TrackFormatSlot(val kind: MediaKind) {
    private val lock = Any()
    private val delivery = CompletableDeferred<CompleteTrackFormat>()

    // The held state, written once, under the lock.
    private var held: CompleteTrackFormat? = null
    private var failed: TrackFormatRefused? = null
    private var unexpected: Throwable? = null

    val format: Deferred<CompleteTrackFormat> get() = delivery

    fun offer(verdict: TrackFormatVerdict): SlotOffer {
        val offered =
            when (verdict) {
                is TrackFormatVerdict.Complete -> verdict.format.kind
                is TrackFormatVerdict.Refused -> verdict.kind
            }
        require(offered == kind) { "a $offered verdict was offered to the $kind slot" }
        // State selection: one short critical section, which records the
        // first verdict or classifies against what is recorded.
        val selected: SlotOffer =
            synchronized(lock) {
                unexpected?.let { return SlotOffer.AlreadyFailed(it) }
                failed?.let { return SlotOffer.AlreadyRefused }
                held?.let { return classify(it, verdict) }
                when (verdict) {
                    is TrackFormatVerdict.Complete -> SlotOffer.Resolved(verdict.format).also { held = verdict.format }
                    is TrackFormatVerdict.Refused ->
                        SlotOffer.Refused(TrackFormatRefused(kind, verdict.refusal)).also { failed = it.failure }
                }
            }
        // Delivery: only the offer that recorded the first verdict, and only
        // after the lock is released.
        val delivered =
            when (selected) {
                is SlotOffer.Resolved -> delivery.complete(selected.format)
                is SlotOffer.Refused -> delivery.completeExceptionally(selected.failure)
                else -> error("only a first verdict is delivered")
            }
        check(delivered) { "the $kind slot's deferred was completed twice" }
        return selected
    }

    /**
     * Selects an unexpected failure -- anything the validation raised that is
     * not a typed verdict -- only if nothing was ever selected: no value, no
     * typed refusal, no earlier failure. The winner records the EXACT [cause]
     * under the lock, then fails the deferred with it outside the lock, and
     * returns true. A losing call returns false and changes and delivers
     * nothing, even while the winner is still delivering; no later offer
     * repairs the failure. Later offers report it as [SlotOffer.AlreadyFailed].
     */
    fun fail(cause: Throwable): Boolean {
        synchronized(lock) {
            if (held != null || failed != null || unexpected != null) return false
            unexpected = cause
        }
        // Delivery: only the winner, and only after the lock is released.
        check(delivery.completeExceptionally(cause)) { "the $kind slot's deferred was completed twice" }
        return true
    }

    private fun classify(held: CompleteTrackFormat, verdict: TrackFormatVerdict): SlotOffer =
        when (verdict) {
            is TrackFormatVerdict.Complete -> if (verdict.format.config == held.config) SlotOffer.Equal else SlotOffer.Changed(held, verdict)
            is TrackFormatVerdict.Refused -> SlotOffer.Changed(held, verdict)
        }
}
