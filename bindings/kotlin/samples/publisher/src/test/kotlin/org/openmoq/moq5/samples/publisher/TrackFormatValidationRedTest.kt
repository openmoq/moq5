package org.openmoq.moq5.samples.publisher

import java.nio.ByteBuffer
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNotSame
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import org.openmoq.moq5.CodecConfigFormat
import org.openmoq.moq5.CodecConfigSource
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.SampleEntry
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.TrackName

/**
 * The format validator in front of track admission: an observed output
 * format -- declared encoder facts plus its codec-specific data -- to a
 * COMPLETE track format carrying its comparison authority, or a named
 * refusal; and the one slot per track that feeds the format wait.
 *
 * `g` rows are controls over the accepted AVC/AAC steps. `f` and `b` rows
 * measure [TrackFormats] against SCRIPTED utilities that log every call and
 * answer from declared responses the sample cannot derive itself; `b` rows
 * pin the declared maximum bitrate. `d` rows measure refusal text over
 * verdicts from the accepted steps. `s` rows measure [TrackFormatSlot],
 * including its reentrant and concurrent offers. `w` rows run the real
 * format wait, owner and coordinator ([Rig]) with the slots as the sources.
 * The real native utility is the host lane
 * (`host.TrackFormatHostIntegrationTest`).
 */
@OptIn(ExperimentalCoroutinesApi::class)
class TrackFormatValidationRedTest {

    private fun hex(s: String): ByteArray = ByteArray(s.length / 2) { s.substring(2 * it, 2 * it + 2).toInt(16).toByte() }

    private fun cat(vararg parts: ByteArray): ByteArray = parts.fold(ByteArray(0)) { a, b -> a + b }

    private fun buffers(vararg units: ByteArray): List<ByteBuffer> = units.map { ByteBuffer.wrap(it) }

    private val sc = hex("00000001")
    private val spsA = hex("6742001eabcd")
    private val spsB = hex("6742001fabcd")
    private val ppsA = hex("68ce3c80")
    private val ppsB = hex("68ce3c81")

    // AudioSpecificConfig: AAC LC (00010), frequency index 4 = 44.1 kHz (0100), 2 channels (0010), flags 000.
    private val asc441 = hex("1210")

    private val videoName = TrackName("video".encodeToByteArray())
    private val audioName = TrackName("audio".encodeToByteArray())
    private val bounds = ConfigBounds(maxUnits = 4, maxUnitBytes = 4096, maxTotalBytes = 8192)

    /** A record and strings the sample cannot derive: only the scripted utilities return them. */
    private val scriptedRecord = hex("a1b2c3d4e5")

    /** A second, DIFFERENT scripted record and string, for rows that need two distinct configurations. */
    private val otherRecord = hex("a1b2c3d4e6")

    /**
     * Answers each build/format pair from its own declared response, in
     * order; a call past the last response fails the row. The responses are
     * fixture inputs, never derived from the bytes under test.
     */
    private class AvcScript(vararg responses: Pair<UtilityOutcome<MediaBytes>, UtilityOutcome<String>>) : AvcRecordUtility {
        private val builds = ArrayDeque(responses.map { it.first })
        private val formats = ArrayDeque(responses.map { it.second })
        val log = mutableListOf<String>()
        val built = mutableListOf<ByteArray>()

        override fun buildInitData(source: CodecConfigSource, bytes: MediaBytes, nalLengthSize: Int): UtilityOutcome<MediaBytes> {
            log += "build $source $nalLengthSize"
            built += bytes.bytes()
            return builds.removeFirstOrNull() ?: throw AssertionError("no scripted build response left")
        }

        override fun formatCodecString(format: CodecConfigFormat, sampleEntry: SampleEntry, decoderConfig: MediaBytes): UtilityOutcome<String> {
            log += "format $format ${sampleEntry.code}"
            return formats.removeFirstOrNull() ?: throw AssertionError("no scripted format response left")
        }
    }

    /**
     * A configuration list that must not be read: every access is counted and
     * fails. A validator that looked at the configuration before rejecting a
     * declared fact fails its row here.
     */
    private class Untouchable : AbstractList<ByteBuffer>() {
        var reads = 0

        override val size: Int
            get() {
                reads++
                throw AssertionError("the configuration was read before the declared facts were checked")
            }

        override fun get(index: Int): ByteBuffer {
            reads++
            throw AssertionError("configuration unit $index was read before the declared facts were checked")
        }
    }

    private class AacScript(private val answer: UtilityOutcome<String>) : AacStringUtility {
        val formatted = mutableListOf<ByteArray>()

        override fun formatCodecString(audioSpecificConfig: MediaBytes): UtilityOutcome<String> {
            formatted += audioSpecificConfig.bytes()
            return answer
        }
    }

    private fun avc() = AvcScript(UtilityOutcome.Done(MediaBytes(scriptedRecord)) to UtilityOutcome.Done("avc1.scripted"))

    private fun aac() = AacScript(UtilityOutcome.Done("mp4a.scripted"))

    private fun twoSets() = buffers(cat(sc, spsA, sc, spsB), cat(sc, ppsA, sc, ppsB))

    // Declared fixture maxima, deliberately different from the diagnostic targets.
    private fun video(
        width: Int? = 640,
        height: Int? = 480,
        target: Int? = 1_000_000,
        maximum: ULong? = 1_500_000uL,
        config: List<ByteBuffer> = twoSets(),
    ) = VideoFormatObservation(width, height, target, maximum, config)

    private fun audio(
        rate: Int? = 44_100,
        channels: Int? = 2,
        target: Int? = 96_000,
        maximum: ULong? = 128_000uL,
        config: List<ByteBuffer> = buffers(asc441),
    ) = AudioFormatObservation(rate, channels, target, maximum, config)

    /** The validated video configuration, declared field by field. */
    private val expectedVideo =
        SendTrackConfig(
            name = videoName,
            mediaType = MediaType.Video,
            packaging = PackagingKind.Raw,
            codec = MediaBytes("avc1.scripted".encodeToByteArray()),
            bitrate = 1_500_000uL,
            initData = MediaBytes(scriptedRecord),
            width = 640u,
            height = 480u,
        )

    private val expectedAudio =
        SendTrackConfig(
            name = audioName,
            mediaType = MediaType.Audio,
            packaging = PackagingKind.Raw,
            codec = MediaBytes("mp4a.scripted".encodeToByteArray()),
            bitrate = 128_000uL,
            initData = MediaBytes(asc441),
            samplerate = 44_100u,
            channelConfig = MediaBytes("2".encodeToByteArray()),
        )

    private fun refusedVideo(observed: VideoFormatObservation, expected: TrackFormatRefusal, script: AvcScript = avc()): AvcScript {
        val v = assertIs<TrackFormatVerdict.Refused>(TrackFormats.video(videoName, observed, bounds, script), "refused: ${expected.description}")
        assertEquals(MediaKind.Video, v.kind)
        assertEquals(expected, v.refusal)
        return script
    }

    private fun refusedAudio(observed: AudioFormatObservation, expected: TrackFormatRefusal, script: AacScript = aac()): AacScript {
        val v = assertIs<TrackFormatVerdict.Refused>(TrackFormats.audio(audioName, observed, bounds, script), "refused: ${expected.description}")
        assertEquals(MediaKind.Audio, v.kind)
        assertEquals(expected, v.refusal)
        return script
    }

    private fun completeVideo(observed: VideoFormatObservation = video(), script: AvcScript = avc()): CompleteTrackFormat =
        assertIs<TrackFormatVerdict.Complete>(TrackFormats.video(videoName, observed, bounds, script)).format

    private fun completeAudio(observed: AudioFormatObservation = audio()): CompleteTrackFormat =
        assertIs<TrackFormatVerdict.Complete>(TrackFormats.audio(audioName, observed, bounds, aac())).format

    // ---- g: controls over the accepted steps ---------------------------------------

    @Test
    fun `g1 the vectors are what the accepted steps say they are`() {
        val snap = assertIs<RawSnapshotResult.Taken>(RawConfigSnapshots.capture(twoSets(), bounds)).snapshot
        val c = assertIs<AvcConfigVerdict.Resolved>(AvcConfiguration.resolve(snap, avc())).config
        assertEquals(2, c.parameterSets.sequenceParameterSets)
        assertContentEquals(spsB, c.parameterSets.sequenceParameterSet(1))
        val asc = assertIs<RawSnapshotResult.Taken>(RawConfigSnapshots.capture(buffers(asc441), bounds)).snapshot
        val facts = assertIs<AacConfigVerdict.Supported>(AacConfigSemantics.examine(asc, DeclaredAudioFacts(44_100, 2))).facts
        assertEquals(44_100, facts.sampleRate)
        assertEquals(2, facts.channelCount)
        assertEquals(1024, facts.framesPerAccessUnit)
        // 00010 0100 0010 000 plus eight zero bits: the padding the accepted subset refuses.
        assertIs<AacConfigVerdict.Unsupported>(AacConfigSemantics.examine(
            assertIs<RawSnapshotResult.Taken>(RawConfigSnapshots.capture(buffers(hex("121000")), bounds)).snapshot,
            DeclaredAudioFacts(44_100, 2),
        ))
    }

    @Test
    fun `g2 the expected configurations are valid public values`() {
        assertEquals(MediaType.Video, expectedVideo.mediaType)
        assertEquals(MediaType.Audio, expectedAudio.mediaType)
    }

    // ---- f: the validator ---------------------------------------------------------

    @Test
    fun `f1 a complete video observation supplies every declared field and carries its resolved authority`() {
        val script = avc()
        val format = assertIs<TrackFormatVerdict.Complete>(TrackFormats.video(videoName, video(), bounds, script)).format
        assertEquals(MediaKind.Video, format.kind)
        assertEquals(expectedVideo, format.config, "codec string and record from the utility; width, height and the declared MAXIMUM bitrate")
        assertEquals(listOf("build AvcAnnexB 4", "format AvcRecord avc1"), script.log)
        assertContentEquals(cat(sc, spsA, sc, spsB, sc, ppsA, sc, ppsB), script.built.single())
        val authority = assertIs<FormatAuthority.Avc>(format.authority).config
        assertContentEquals(scriptedRecord, authority.record.bytes())
        assertEquals("avc1.scripted", authority.codecString)
        assertEquals(2, authority.parameterSets.sequenceParameterSets)
        assertEquals(2, authority.parameterSets.pictureParameterSets)
        assertContentEquals(spsB, authority.parameterSets.sequenceParameterSet(1), "the non-first SPS")
        assertContentEquals(ppsB, authority.parameterSets.pictureParameterSet(1), "the non-first PPS")
    }

    @Test
    fun `f2 a partial video observation is refused by name before the configuration is read or a utility called`() {
        fun untouched(observed: (Untouchable) -> VideoFormatObservation, expected: TrackFormatRefusal) {
            val config = Untouchable()
            assertEquals(emptyList(), refusedVideo(observed(config), expected).log)
            assertEquals(0, config.reads, "no configuration access")
        }
        untouched({ video(width = null, config = it) }, TrackFormatRefusal.MissingFact("width"))
        untouched({ video(height = null, config = it) }, TrackFormatRefusal.MissingFact("height"))
        untouched({ video(maximum = null, config = it) }, TrackFormatRefusal.MissingFact("maximum bitrate"))
        // Facts are checked in declared order: width first.
        untouched({ video(width = null, height = null, maximum = null, config = it) }, TrackFormatRefusal.MissingFact("width"))
        assertEquals(emptyList(), refusedVideo(video(config = emptyList()), TrackFormatRefusal.Snapshot("no configuration units")).log)
    }

    @Test
    fun `f3 invalid declared video facts are refused by name before the configuration is read`() {
        for ((observed, expected) in listOf<Pair<(Untouchable) -> VideoFormatObservation, TrackFormatRefusal>>(
            { c: Untouchable -> video(width = 0, config = c) } to TrackFormatRefusal.InvalidFact("width", 0),
            { c: Untouchable -> video(height = -1, config = c) } to TrackFormatRefusal.InvalidFact("height", -1),
            { c: Untouchable -> video(maximum = 0uL, config = c) } to TrackFormatRefusal.ZeroMaximumBitrate,
        )) {
            val config = Untouchable()
            assertEquals(emptyList(), refusedVideo(observed(config), expected).log)
            assertEquals(0, config.reads)
        }
    }

    @Test
    fun `b1 the video catalog bitrate is the declared maximum only - never the target`() {
        // A target with no maximum is refused: the target satisfies nothing.
        refusedVideo(video(target = 1_000_000, maximum = null), TrackFormatRefusal.MissingFact("maximum bitrate"))
        // Different target and maximum: only the maximum is emitted.
        assertEquals(1_500_000uL, completeVideo(video(target = 1_000_000, maximum = 1_500_000uL)).config.bitrate)
        // Varying or omitting the target alone changes nothing in the validated configuration.
        assertEquals(expectedVideo, completeVideo(video(target = null)).config)
        assertEquals(expectedVideo, completeVideo(video(target = 7)).config)
        assertEquals(expectedVideo, completeVideo(video(target = -5)).config, "a diagnostic target is not validated")
        // A maximum above Int.MAX_VALUE, and at the top of the unsigned range, is carried unnarrowed.
        assertEquals(5_000_000_000uL, completeVideo(video(maximum = 5_000_000_000uL)).config.bitrate)
        assertEquals(ULong.MAX_VALUE, completeVideo(video(maximum = ULong.MAX_VALUE)).config.bitrate)
    }

    @Test
    fun `b2 the audio catalog bitrate is the declared maximum only - never the target`() {
        refusedAudio(audio(target = 96_000, maximum = null), TrackFormatRefusal.MissingFact("maximum bitrate"))
        refusedAudio(audio(maximum = 0uL), TrackFormatRefusal.ZeroMaximumBitrate)
        assertEquals(128_000uL, completeAudio(audio(target = 96_000, maximum = 128_000uL)).config.bitrate)
        assertEquals(expectedAudio, completeAudio(audio(target = null)).config)
        assertEquals(expectedAudio, completeAudio(audio(target = 1)).config)
        assertEquals(3_000_000_000uL, completeAudio(audio(maximum = 3_000_000_000uL)).config.bitrate)
        // Above 2^32 as well, so a 32-bit narrowing is not hidden by the unsigned reinterpretation.
        assertEquals(5_000_000_000uL, completeAudio(audio(maximum = 5_000_000_000uL)).config.bitrate)
        assertEquals(ULong.MAX_VALUE, completeAudio(audio(maximum = ULong.MAX_VALUE)).config.bitrate)
    }

    @Test
    fun `f4 a video configuration refused by the accepted step is carried as that verdict`() {
        fun avcRefusal(observed: VideoFormatObservation, script: AvcScript = avc()): Pair<AvcConfigVerdict, AvcScript> {
            val v = assertIs<TrackFormatVerdict.Refused>(TrackFormats.video(videoName, observed, bounds, script))
            return assertIs<TrackFormatRefusal.Avc>(v.refusal).verdict to script
        }
        // csd-1 absent: one unit, refused by the resolver's own subset.
        val (one, s1) = avcRefusal(video(config = buffers(cat(sc, spsA))))
        assertEquals(AvcSubsetRefusal.UnitCount(1), assertIs<AvcConfigVerdict.OutsideSubset>(one).refusal)
        assertEquals(emptyList(), s1.log)
        val (bad, s2) = avcRefusal(video(config = buffers(cat(sc, hex("e742001eabcd")), cat(sc, ppsA))))
        assertEquals(AvcDefect.ForbiddenBitSet(0, 0), assertIs<AvcConfigVerdict.Malformed>(bad).defect)
        assertEquals(emptyList(), s2.log)
        val (refused, s3) = avcRefusal(video(), AvcScript(UtilityOutcome.Refused(MoqResultCode(-3)) to UtilityOutcome.Done("unused")))
        val r = assertIs<AvcConfigVerdict.UtilityRefused>(refused)
        assertEquals(UtilityStage.BUILD, r.stage)
        assertEquals(-3, r.code.value)
        assertEquals(listOf("build AvcAnnexB 4"), s3.log)
    }

    @Test
    fun `f5 a configuration over its capture bound is refused before it is validated`() {
        val tight = ConfigBounds(maxUnits = 4, maxUnitBytes = 8, maxTotalBytes = 8192)
        val script = avc()
        val v = assertIs<TrackFormatVerdict.Refused>(TrackFormats.video(videoName, video(), tight, script))
        assertEquals(TrackFormatRefusal.Snapshot("configuration unit 0 of 20 bytes exceeds the limit of 8"), v.refusal)
        assertEquals(emptyList(), script.log)
    }

    @Test
    fun `f6 a supported audio observation supplies every declared field, and its facts key the track`() {
        val script = aac()
        val format = assertIs<TrackFormatVerdict.Complete>(TrackFormats.audio(audioName, audio(), bounds, script)).format
        assertEquals(MediaKind.Audio, format.kind)
        assertEquals(expectedAudio, format.config, "codec string from the utility; samplerate and channelConfig from the supported facts; initData the exact configuration")
        assertContentEquals(asc441, script.formatted.single(), "the utility formats exactly the validated configuration")
        val facts = assertIs<FormatAuthority.Aac>(format.authority).facts
        assertEquals(44_100, facts.sampleRate)
        assertEquals(2, facts.channelCount)
        assertEquals(1024, facts.framesPerAccessUnit)
        // Keying through the ACCEPTED result: unit 1 is 1024 samples at 44.1 kHz, floored once.
        val keying = AacConfigSemantics.keyingFor(facts, 0L)
        assertEquals(AudioKeyOutcome.Key(23_219L), keying.keyFor(1L))
    }

    @Test
    fun `f7 an audio observation that is partial, disagrees, or is outside the subset is refused with no utility call`() {
        for ((observed, expected) in listOf<Pair<(Untouchable) -> AudioFormatObservation, TrackFormatRefusal>>(
            { c: Untouchable -> audio(rate = null, config = c) } to TrackFormatRefusal.MissingFact("sample rate"),
            { c: Untouchable -> audio(channels = null, config = c) } to TrackFormatRefusal.MissingFact("channel count"),
            { c: Untouchable -> audio(maximum = null, config = c) } to TrackFormatRefusal.MissingFact("maximum bitrate"),
            { c: Untouchable -> audio(channels = 0, config = c) } to TrackFormatRefusal.InvalidFact("channel count", 0),
            { c: Untouchable -> audio(rate = 0, config = c) } to TrackFormatRefusal.InvalidFact("sample rate", 0),
            { c: Untouchable -> audio(rate = -44_100, config = c) } to TrackFormatRefusal.InvalidFact("sample rate", -44_100),
            { c: Untouchable -> audio(maximum = 0uL, config = c) } to TrackFormatRefusal.ZeroMaximumBitrate,
        )) {
            val config = Untouchable()
            assertEquals(emptyList(), refusedAudio(observed(config), expected).formatted)
            assertEquals(0, config.reads, "no configuration access")
        }
        fun aacRefusal(observed: AudioFormatObservation): AacConfigVerdict {
            val script = aac()
            val v = assertIs<TrackFormatVerdict.Refused>(TrackFormats.audio(audioName, observed, bounds, script))
            assertEquals(emptyList(), script.formatted)
            return assertIs<TrackFormatRefusal.Aac>(v.refusal).verdict
        }
        val mismatched = assertIs<AacConfigVerdict.Mismatched>(aacRefusal(audio(rate = 48_000)))
        assertTrue(mismatched.sampleRateDiffers)
        // Trailing padding is outside the accepted subset: not trimmed, not tolerated.
        val padded = assertIs<AacConfigVerdict.Unsupported>(aacRefusal(audio(config = buffers(hex("121000")))))
        assertEquals(listOf<AscFeature>(AscFeature.TrailingData(8)), padded.features)
        assertEquals(AscFeature.UnitCount(2), assertIs<AacConfigVerdict.Unsupported>(aacRefusal(audio(config = buffers(asc441, asc441)))).features.single())
    }

    @Test
    fun `f8 a refused AAC codec string is a named refusal`() {
        val script = AacScript(UtilityOutcome.Refused(MoqResultCode(-3)))
        refusedAudio(audio(), TrackFormatRefusal.AacString(MoqResultCode(-3)), script)
        assertContentEquals(asc441, script.formatted.single())
    }

    // ---- d: refusal text from the typed detail --------------------------------------

    private fun snapshot(vararg units: ByteArray): RawConfigSnapshot =
        assertIs<RawSnapshotResult.Taken>(RawConfigSnapshots.capture(buffers(*units), bounds)).snapshot

    private fun described(kind: MediaKind, refusal: TrackFormatRefusal, expected: String) {
        assertEquals(expected, refusal.description)
        assertEquals("the $kind format was refused: $expected", TrackFormatRefused(kind, refusal).message)
    }

    @Test
    fun `d1 an AVC refusal names its defect, subset refusal, or utility stage and code`() {
        val malformed = AvcConfiguration.resolve(snapshot(cat(sc, hex("e742001eabcd")), cat(sc, ppsA)), avc())
        described(
            MediaKind.Video,
            TrackFormatRefusal.Avc(assertIs<AvcConfigVerdict.Malformed>(malformed)),
            "the AVC configuration is malformed: unit 0 at 0: forbidden_zero_bit is 1",
        )
        val subset = AvcConfiguration.resolve(snapshot(cat(sc, spsA)), avc())
        described(
            MediaKind.Video,
            TrackFormatRefusal.Avc(assertIs<AvcConfigVerdict.OutsideSubset>(subset)),
            "the AVC configuration is outside the sample's subset: 1 configuration units; the sample reads csd-0 and csd-1",
        )
        val utility =
            AvcConfiguration.resolve(
                snapshot(cat(sc, spsA), cat(sc, ppsA)),
                AvcScript(UtilityOutcome.Done(MediaBytes(scriptedRecord)) to UtilityOutcome.Refused(MoqResultCode(-2))),
            )
        described(
            MediaKind.Video,
            TrackFormatRefusal.Avc(assertIs<AvcConfigVerdict.UtilityRefused>(utility)),
            "the codec utility refused the AVC configuration at FORMAT with code -2",
        )
    }

    @Test
    fun `d2 an AAC refusal names its shortage, features, or the disagreeing facts`() {
        val short = AacConfigSemantics.examine(snapshot(hex("12")), DeclaredAudioFacts(44_100, 2))
        described(
            MediaKind.Audio,
            TrackFormatRefusal.Aac(assertIs<AacConfigVerdict.Malformed>(short)),
            "the AAC configuration is malformed: the configuration ends at bit 8, before its SAMPLING_FREQUENCY_INDEX (which needs 9)",
        )
        val padded = AacConfigSemantics.examine(snapshot(hex("121000")), DeclaredAudioFacts(44_100, 2))
        described(
            MediaKind.Audio,
            TrackFormatRefusal.Aac(assertIs<AacConfigVerdict.Unsupported>(padded)),
            "the AAC configuration is outside the sample's subset: 8 bit(s) after the configuration that are not a 0x2b7 extension",
        )
        val mismatched = AacConfigSemantics.examine(snapshot(asc441), DeclaredAudioFacts(48_000, 1))
        described(
            MediaKind.Audio,
            TrackFormatRefusal.Aac(assertIs<AacConfigVerdict.Mismatched>(mismatched)),
            "the AAC configuration says 44100 Hz and 2 channel(s); the encoder declared 48000 Hz and 1 channel(s)",
        )
        described(MediaKind.Audio, TrackFormatRefusal.AacString(MoqResultCode(-3)), "the codec utility refused the AAC codec string with code -3")
    }

    @Test
    fun `d3 declared-fact refusals are named`() {
        described(MediaKind.Video, TrackFormatRefusal.MissingFact("maximum bitrate"), "the output format declared no maximum bitrate")
        described(MediaKind.Video, TrackFormatRefusal.InvalidFact("width", 0), "the output format declared width 0")
        described(MediaKind.Audio, TrackFormatRefusal.ZeroMaximumBitrate, "the declared maximum bitrate is 0")
    }

    // ---- s: the slot ----------------------------------------------------------------

    @Test
    fun `s1 the first complete verdict settles the slot with that exact format`() {
        val slot = TrackFormatSlot(MediaKind.Video)
        assertFalse(slot.format.isCompleted)
        val first = completeVideo()
        assertSame(first, assertIs<SlotOffer.Resolved>(slot.offer(TrackFormatVerdict.Complete(first))).format)
        assertSame(first, slot.format.getCompleted())
    }

    @Test
    fun `s2 a later observation is classified against the held format and never adopted`() {
        val changedPps = hex("68ce3c82")
        assertEquals(ppsB.size, changedPps.size, "setup: the changed PPS has the same length")
        // Three separately resolved formats, each from its OWN declared utility response.
        val first = completeVideo(script = AvcScript(UtilityOutcome.Done(MediaBytes(scriptedRecord)) to UtilityOutcome.Done("avc1.scripted")))
        val equal = completeVideo(script = AvcScript(UtilityOutcome.Done(MediaBytes(scriptedRecord)) to UtilityOutcome.Done("avc1.scripted")))
        val other =
            completeVideo(
                video(config = buffers(cat(sc, spsA, sc, spsB), cat(sc, ppsA, sc, changedPps))),
                // Only the record differs: the codec string is the SAME declared response.
                AvcScript(UtilityOutcome.Done(MediaBytes(otherRecord)) to UtilityOutcome.Done("avc1.scripted")),
            )
        val wider = completeVideo(video(width = 1280))
        // Fixture preconditions, before any offer.
        assertNotSame(first, equal)
        assertEquals(first.config, equal.config, "setup: the equal control is equal")
        assertNotEquals(first.config, other.config, "setup: the changed PPS carries a different declared record")
        assertEquals(first.config, other.config.copy(initData = first.config.initData), "setup: the record is the ONLY difference")
        assertNotEquals(first.config, wider.config, "setup: a different declared width")

        val slot = TrackFormatSlot(MediaKind.Video)
        assertSame(first, assertIs<SlotOffer.Resolved>(slot.offer(TrackFormatVerdict.Complete(first))).format)
        assertSame(SlotOffer.Equal, slot.offer(TrackFormatVerdict.Complete(equal)))
        assertSame(first, slot.format.getCompleted())
        val otherVerdict = TrackFormatVerdict.Complete(other)
        val changed = assertIs<SlotOffer.Changed>(slot.offer(otherVerdict))
        assertSame(first, changed.held)
        assertSame(otherVerdict, changed.observed)
        assertSame(first, slot.format.getCompleted())
        val widerVerdict = TrackFormatVerdict.Complete(wider)
        val widened = assertIs<SlotOffer.Changed>(slot.offer(widerVerdict))
        assertSame(first, widened.held)
        assertSame(widerVerdict, widened.observed)
        assertSame(first, slot.format.getCompleted())
        val refused = TrackFormatVerdict.Refused(MediaKind.Video, TrackFormatRefusal.MissingFact("width"))
        val refusedLater = assertIs<SlotOffer.Changed>(slot.offer(refused))
        assertSame(first, refusedLater.held)
        assertSame(refused, refusedLater.observed)
        assertSame(first, slot.format.getCompleted(), "nothing changed the settled value")
    }

    @Test
    fun `s3 a refused first verdict fails the slot, and no later verdict repairs it`() {
        val slot = TrackFormatSlot(MediaKind.Audio)
        val refusal = TrackFormatRefusal.MissingFact("channel count")
        val failure = assertIs<SlotOffer.Refused>(slot.offer(TrackFormatVerdict.Refused(MediaKind.Audio, refusal))).failure
        assertEquals(MediaKind.Audio, failure.kind)
        assertEquals(refusal, failure.refusal)
        assertSame(failure, slot.format.getCompletionExceptionOrNull(), "the slot fails with that exact instance")
        assertSame(SlotOffer.AlreadyRefused, slot.offer(TrackFormatVerdict.Complete(completeAudio())))
        assertSame(failure, slot.format.getCompletionExceptionOrNull())
    }

    @Test
    fun `s4 a verdict of the other kind is a contract failure that settles nothing`() {
        // Valid inputs, built BEFORE the contract check is exercised.
        val audioFormat = completeAudio()
        val wrongComplete = TrackFormatVerdict.Complete(audioFormat)
        val wrongRefused = TrackFormatVerdict.Refused(MediaKind.Audio, TrackFormatRefusal.MissingFact("sample rate"))
        val right = completeVideo()
        val slot = TrackFormatSlot(MediaKind.Video)
        assertFailsWith<IllegalArgumentException> { slot.offer(wrongComplete) }
        assertFalse(slot.format.isCompleted)
        assertFailsWith<IllegalArgumentException> { slot.offer(wrongRefused) }
        assertFalse(slot.format.isCompleted)
        // The unchanged slot still settles on its first correct verdict.
        assertSame(right, assertIs<SlotOffer.Resolved>(slot.offer(TrackFormatVerdict.Complete(right))).format)
        assertSame(right, slot.format.getCompleted())
    }

    /** Records what a completion handler did, so the test owner asserts it; a handler never throws into the completion. */
    private class HandlerLedger {
        val offers = java.util.Collections.synchronizedList(mutableListOf<SlotOffer>())
        val failures = java.util.Collections.synchronizedList(mutableListOf<Throwable>())

        /**
         * Runs [block] and records whatever it raised -- a cancellation, an
         * exception or an error, as the same instance -- for the test owner
         * to assert after settlement. Nothing is thrown into the completion.
         */
        fun run(block: () -> Unit) {
            runCatching(block).exceptionOrNull()?.let { failures += it }
        }
    }

    private fun reentrantOfferIsClassified(slot: TrackFormatSlot, first: TrackFormatVerdict, again: TrackFormatVerdict, expected: SlotOffer) {
        val ledger = HandlerLedger()
        // A completion handler runs synchronously inside the completing offer.
        slot.format.invokeOnCompletion { ledger.run { ledger.offers += slot.offer(again) } }
        val outer = slot.offer(first)
        assertEquals(emptyList(), ledger.failures.toList(), "the handler raised nothing")
        assertEquals(listOf(expected), ledger.offers.toList(), "the reentrant offer was classified against the recorded state")
        when (first) {
            is TrackFormatVerdict.Complete -> {
                assertSame(first.format, assertIs<SlotOffer.Resolved>(outer).format)
                assertSame(first.format, slot.format.getCompleted())
            }
            is TrackFormatVerdict.Refused -> assertSame(assertIs<SlotOffer.Refused>(outer).failure, slot.format.getCompletionExceptionOrNull())
        }
    }

    @Test
    fun `s5 an offer made from inside the settling completion is classified against the recorded value`() {
        reentrantOfferIsClassified(
            TrackFormatSlot(MediaKind.Video),
            TrackFormatVerdict.Complete(completeVideo()),
            TrackFormatVerdict.Complete(completeVideo()),
            SlotOffer.Equal,
        )
    }

    @Test
    fun `s5b a reentrant offer after a refused first verdict is classified as already refused`() {
        reentrantOfferIsClassified(
            TrackFormatSlot(MediaKind.Audio),
            TrackFormatVerdict.Refused(MediaKind.Audio, TrackFormatRefusal.MissingFact("sample rate")),
            TrackFormatVerdict.Complete(completeAudio()),
            SlotOffer.AlreadyRefused,
        )
    }

    /**
     * The progress edge: while the settling offer's completion handler is
     * still ACTIVE, an offer from another thread must finish and report its
     * exact classification. The handler waits for that report on a latch
     * with a failure bound only; a slot that completes under its lock makes
     * the contender wait for the handler, the bound expires, the failure is
     * recorded, and the handler is released -- finitely. Every created
     * thread is joined on every path before anything is asserted.
     */
    private fun concurrentOfferFinishesWhileDeliveryIsActive(slot: TrackFormatSlot, first: TrackFormatVerdict, later: TrackFormatVerdict, expected: SlotOffer) {
        val ledger = HandlerLedger()
        val reported = java.util.concurrent.CountDownLatch(1)
        val contenderResult = java.util.concurrent.atomic.AtomicReference<Result<SlotOffer>?>()
        val contender =
            Thread({
                try {
                    contenderResult.set(runCatching { slot.offer(later) })
                } finally {
                    reported.countDown()
                }
            }, "slot-contender")
        var reportedWhileActive = false
        slot.format.invokeOnCompletion {
            ledger.run {
                contender.start()
                reportedWhileActive = reported.await(BOUND_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
                if (!reportedWhileActive) throw AssertionError("the contending offer did not finish while the completion handler was active")
            }
        }
        val outer: Result<SlotOffer>
        try {
            outer = runCatching { slot.offer(first) }
        } finally {
            if (contender.state != Thread.State.NEW) contender.join(BOUND_SECONDS * 1_000L)
        }
        assertFalse(contender.isAlive, "the contender was joined")
        assertEquals(emptyList(), ledger.failures.toList(), "the handler recorded no failure")
        assertTrue(reportedWhileActive)
        assertSame(expected, assertNotNull(contenderResult.get()).getOrThrow(), "the contender's exact classification")
        val settled = outer.getOrThrow()
        when (first) {
            is TrackFormatVerdict.Complete -> {
                assertSame(first.format, assertIs<SlotOffer.Resolved>(settled).format)
                assertSame(first.format, slot.format.getCompleted())
            }
            is TrackFormatVerdict.Refused -> assertSame(assertIs<SlotOffer.Refused>(settled).failure, slot.format.getCompletionExceptionOrNull())
        }
    }

    @Test
    fun `s6 a concurrent offer finishes while the first offer's delivery is still active`() {
        concurrentOfferFinishesWhileDeliveryIsActive(
            TrackFormatSlot(MediaKind.Video),
            TrackFormatVerdict.Complete(completeVideo()),
            TrackFormatVerdict.Complete(completeVideo()),
            SlotOffer.Equal,
        )
    }

    @Test
    fun `s6b a concurrent offer after a refused first verdict finishes while its delivery is active`() {
        concurrentOfferFinishesWhileDeliveryIsActive(
            TrackFormatSlot(MediaKind.Audio),
            TrackFormatVerdict.Refused(MediaKind.Audio, TrackFormatRefusal.MissingFact("channel count")),
            TrackFormatVerdict.Complete(completeAudio()),
            SlotOffer.AlreadyRefused,
        )
    }

    /**
     * A failure raised inside a completion handler, of any category, is
     * RECORDED for the test owner -- the same instance -- and nothing
     * escapes into the completing offer, whose value is still delivered.
     */
    private fun handlerFailureIsRecorded(raised: Throwable) {
        val first = completeVideo()
        val slot = TrackFormatSlot(MediaKind.Video)
        val ledger = HandlerLedger()
        slot.format.invokeOnCompletion { ledger.run { throw raised } }
        val outer = runCatching { slot.offer(TrackFormatVerdict.Complete(first)) }
        assertEquals(null, outer.exceptionOrNull(), "nothing escaped into the completing offer")
        assertSame(first, assertIs<SlotOffer.Resolved>(outer.getOrThrow()).format)
        assertSame(raised, ledger.failures.single(), "the same instance, recorded")
        assertSame(first, slot.format.getCompleted(), "the value was still delivered")
    }

    @Test
    fun `s7 a cancellation raised in a completion handler is recorded, not thrown into the completion`() {
        handlerFailureIsRecorded(kotlinx.coroutines.CancellationException("the handler was cancelled"))
    }

    @Test
    fun `s7b an exception raised in a completion handler is recorded`() {
        handlerFailureIsRecorded(IllegalStateException("the handler failed"))
    }

    @Test
    fun `s7c an error raised in a completion handler is recorded`() {
        handlerFailureIsRecorded(AssertionError("the handler's assertion failed"))
    }

    // ---- u: an unexpected failure selected through fail(cause) ------------------------

    private fun failsWithExactCause(cause: Throwable) {
        val slot = TrackFormatSlot(MediaKind.Video)
        assertTrue(slot.fail(cause), "the first selection wins")
        assertTrue(slot.format.isCompleted)
        assertSame(cause, slot.format.getCompletionExceptionOrNull(), "the exact retained cause, not a wrapper")
        assertSame(cause, assertIs<SlotOffer.AlreadyFailed>(slot.offer(TrackFormatVerdict.Complete(completeVideo()))).cause)
    }

    @Test
    fun `u1 an unexpected exception fails the slot with that exact instance`() = failsWithExactCause(IllegalStateException("the utility could not be reached"))

    @Test
    fun `u1b an unexpected error fails the slot with that exact instance`() = failsWithExactCause(AssertionError("an Error during validation"))

    @Test
    fun `u1c an unexpected cancellation fails the slot with that exact instance`() = failsWithExactCause(kotlinx.coroutines.CancellationException("validation was cancelled"))

    @Test
    fun `u2 the first failure wins and a later failure changes and delivers nothing`() {
        val slot = TrackFormatSlot(MediaKind.Audio)
        val first = IllegalStateException("first")
        val second = IllegalStateException("second")
        assertTrue(slot.fail(first))
        assertFalse(slot.fail(second), "a losing selection")
        assertSame(first, slot.format.getCompletionExceptionOrNull())
        assertSame(first, assertIs<SlotOffer.AlreadyFailed>(slot.offer(TrackFormatVerdict.Complete(completeAudio()))).cause)
    }

    @Test
    fun `u3 a failure after a resolution or a typed refusal loses, and neither is changed`() {
        val resolved = TrackFormatSlot(MediaKind.Video)
        val first = completeVideo()
        resolved.offer(TrackFormatVerdict.Complete(first))
        assertFalse(resolved.fail(IllegalStateException("too late")))
        assertSame(first, resolved.format.getCompleted())
        assertSame(SlotOffer.Equal, resolved.offer(TrackFormatVerdict.Complete(completeVideo())), "the held format still classifies")

        val refused = TrackFormatSlot(MediaKind.Audio)
        val failure = assertIs<SlotOffer.Refused>(refused.offer(TrackFormatVerdict.Refused(MediaKind.Audio, TrackFormatRefusal.MissingFact("sample rate")))).failure
        assertFalse(refused.fail(IllegalStateException("too late")))
        assertSame(failure, refused.format.getCompletionExceptionOrNull())
        assertSame(SlotOffer.AlreadyRefused, refused.offer(TrackFormatVerdict.Complete(completeAudio())), "the typed refusal still classifies as refused")
    }

    @Test
    fun `u4 no later offer repairs a failed slot, and each reports the exact retained cause`() {
        val slot = TrackFormatSlot(MediaKind.Video)
        val cause = IllegalStateException("the utility failed")
        assertTrue(slot.fail(cause))
        for (verdict in listOf(
            TrackFormatVerdict.Complete(completeVideo()),
            TrackFormatVerdict.Refused(MediaKind.Video, TrackFormatRefusal.MissingFact("width")),
        )) {
            assertSame(cause, assertIs<SlotOffer.AlreadyFailed>(slot.offer(verdict)).cause)
        }
        assertSame(cause, slot.format.getCompletionExceptionOrNull(), "still that failure")
    }

    @Test
    fun `u5 a wrong-kind offer after a failure is still a contract error that changes nothing`() {
        val slot = TrackFormatSlot(MediaKind.Video)
        val cause = IllegalStateException("the utility failed")
        val audio = completeAudio()
        assertTrue(slot.fail(cause))
        assertFailsWith<IllegalArgumentException> { slot.offer(TrackFormatVerdict.Complete(audio)) }
        assertSame(cause, slot.format.getCompletionExceptionOrNull())
    }

    @Test
    fun `u6 a failure inside the delivering completion loses to the selected one, and an offer there reports it`() {
        val slot = TrackFormatSlot(MediaKind.Video)
        val cause = IllegalStateException("selected")
        val again = completeVideo()
        val ledger = HandlerLedger()
        val inner = mutableListOf<Any>()
        slot.format.invokeOnCompletion {
            ledger.run {
                inner += slot.fail(IllegalStateException("reentrant"))
                inner += slot.offer(TrackFormatVerdict.Complete(again))
            }
        }
        assertTrue(slot.fail(cause))
        assertEquals(emptyList(), ledger.failures.toList(), "the handler raised nothing")
        assertEquals(false, inner[0], "the reentrant failure lost")
        assertSame(cause, assertIs<SlotOffer.AlreadyFailed>(inner[1]).cause)
        assertSame(cause, slot.format.getCompletionExceptionOrNull())
    }

    /**
     * The failure is delivered OUTSIDE the lock: while its completion handler
     * is active, another thread's failure and offer finish and report their
     * classification. The bounded latch is a failure bound only; every thread
     * is joined before anything is asserted.
     */
    @Test
    fun `u7 a competing failure and offer finish while the selected failure is still being delivered`() {
        val slot = TrackFormatSlot(MediaKind.Video)
        val cause = IllegalStateException("selected")
        val competing = IllegalStateException("competing")
        val again = completeVideo()
        val ledger = HandlerLedger()
        val reported = java.util.concurrent.CountDownLatch(1)
        val results = java.util.concurrent.atomic.AtomicReference<Result<Pair<Boolean, SlotOffer>>?>()
        val contender =
            Thread({
                try {
                    results.set(runCatching { slot.fail(competing) to slot.offer(TrackFormatVerdict.Complete(again)) })
                } finally {
                    reported.countDown()
                }
            }, "slot-failure-contender")
        var reportedWhileActive = false
        slot.format.invokeOnCompletion {
            ledger.run {
                contender.start()
                reportedWhileActive = reported.await(BOUND_SECONDS, java.util.concurrent.TimeUnit.SECONDS)
                if (!reportedWhileActive) throw AssertionError("the competing calls did not finish while the failure was being delivered")
            }
        }
        val outer: Result<Boolean>
        try {
            outer = runCatching { slot.fail(cause) }
        } finally {
            if (contender.state != Thread.State.NEW) contender.join(BOUND_SECONDS * 1_000L)
        }
        assertFalse(contender.isAlive, "the contender was joined")
        assertEquals(emptyList(), ledger.failures.toList(), "the handler recorded no failure")
        assertTrue(reportedWhileActive)
        val (lost, offered) = assertNotNull(results.get()).getOrThrow()
        assertFalse(lost, "the competing failure lost")
        assertSame(cause, assertIs<SlotOffer.AlreadyFailed>(offered).cause)
        assertTrue(outer.getOrThrow())
        assertSame(cause, slot.format.getCompletionExceptionOrNull())
    }

    @Test
    fun `u8 a failed slot wakes the real pair wait with that failure and nothing is added`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val videoSlot = TrackFormatSlot(MediaKind.Video)
        val audioSlot = TrackFormatSlot(MediaKind.Audio)
        audioSlot.offer(TrackFormatVerdict.Complete(completeAudio()))
        val cause = IllegalStateException("the video validator failed unexpectedly")
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(FormatSources(videoSlot.format, audioSlot.format), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    assertTrue(videoSlot.fail(cause))
                    runCurrent()
                    // Settled at virtual time zero: a failure, not an expired budget.
                    assertEquals(0L, currentTime, "no budget elapsed")
                    expectRoots(expectRecorded(settled(owner), cause), emptyList())
                }
            }
        expectLeft(escaped, cause)
        expectEffects(emptyList(), rig.tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
        assertTrue(rig.completion.calls.isEmpty())
        assertTrue(rig.owner.isDecided)
        assertEquals(1, rig.session.stops)
    }

    // ---- w: the slots as the real format wait's sources --------------------------------

    /** The wait admits nothing until BOTH slots settle, then adds exactly the validated configurations, video first. */
    private suspend fun kotlinx.coroutines.test.TestScope.addsExactlyAfterBoth(firstOffer: MediaKind) {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val videoSlot = TrackFormatSlot(MediaKind.Video)
        val audioSlot = TrackFormatSlot(MediaKind.Audio)
        val v = completeVideo()
        val a = completeAudio()
        fun offer(kind: MediaKind) {
            if (kind == MediaKind.Video) videoSlot.offer(TrackFormatVerdict.Complete(v)) else audioSlot.offer(TrackFormatVerdict.Complete(a))
        }
        withRun({ rig.run(rig.formatPublication(FormatSources(videoSlot.format, audioSlot.format), BUDGET)) }) { owner ->
            runCurrent()
            stillLive(owner)
            expectEffects(emptyList(), rig.tracks.all())
            offer(firstOffer)
            runCurrent()
            stillLive(owner)
            expectEffects(emptyList(), rig.tracks.all())
            assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered after one format")
            offer(if (firstOffer == MediaKind.Video) MediaKind.Audio else MediaKind.Video)
            runCurrent()
            stillLive(owner)
            expectEffects(listOf(Effect.Add(expectedVideo), Effect.Add(expectedAudio)), rig.tracks.all())
            owner.cancel("the row has seen the adds")
            runCurrent()
            onlyIssuedSettlement(owner)
        }
        assertTrue(rig.owner.isDecided, "the owner was settled")
    }

    @Test
    fun `w1a audio validated first - nothing until video, then video and audio adds`() = runTest { addsExactlyAfterBoth(MediaKind.Audio) }

    @Test
    fun `w1b video validated first - nothing until audio, then video and audio adds`() = runTest { addsExactlyAfterBoth(MediaKind.Video) }

    @Test
    fun `w2 a refused format is the run's primary by identity, and nothing is added`() = runTest {
        val rig = Rig(ScriptedTracks().answers(HostTrack("v")).answers(HostTrack("a")))
        val videoSlot = TrackFormatSlot(MediaKind.Video)
        val audioSlot = TrackFormatSlot(MediaKind.Audio)
        audioSlot.offer(TrackFormatVerdict.Complete(completeAudio()))
        lateinit var failure: TrackFormatRefused
        val escaped =
            escaping {
                withRun({ rig.run(rig.formatPublication(FormatSources(videoSlot.format, audioSlot.format), BUDGET)) }) { owner ->
                    runCurrent()
                    stillLive(owner)
                    failure = assertIs<SlotOffer.Refused>(
                        videoSlot.offer(TrackFormatVerdict.Refused(MediaKind.Video, TrackFormatRefusal.MissingFact("height"))),
                    ).failure
                    runCurrent()
                    expectRoots(expectRecorded(settled(owner), failure), emptyList())
                }
            }
        expectLeft(escaped, failure)
        expectEffects(emptyList(), rig.tracks.all())
        assertFalse(rig.videoEntry.entered || rig.audioEntry.entered, "no lane entered")
        assertTrue(rig.completion.calls.isEmpty())
        assertTrue(rig.owner.isDecided, "the owner was settled")
        assertEquals(1, rig.session.stops)
    }

    private companion object {
        val BUDGET = 5.seconds

        /** A failure bound for the cross-thread schedules; never a success criterion. */
        const val BOUND_SECONDS = 10L
    }
}
