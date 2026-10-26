package org.openmoq.moq5.samples.player

import org.openmoq.moq5.ByteRange
import org.openmoq.moq5.CmafSample
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.UnixMicros
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The receive-side media contract, pinned by bytes rather than by prose.
 *
 * Fixtures are FRAMING fixtures: they prove mapping, not decodability.
 *
 * - the AVCDecoderConfigurationRecord is ISO/IEC 14496-15 5.3.3.1. The
 *   29-byte High-profile record below is what the project's own unchanged
 *   builder produces from these parameter sets (measured, 1763 A.2), including
 *   the chroma/bit-depth extension `fd f8 f8 00`. The 25-byte form is the
 *   legacy record a READER still has to tolerate — reader tolerance is not the
 *   writer's oracle.
 * - framing is not declared by the user and not carried by the catalog: a CMAF
 *   fragment's samples are length-prefixed by the container, and a RAW/LOC
 *   payload is disambiguated per draft-ietf-moq-loc-01 2.1.3, where a leading
 *   four-byte value of 1 is the start code, not a NAL length of one.
 * - `12 10` is AAC-LC, sampling frequency index 4 (44100 Hz), channels 2. ADTS
 *   is an explicit application-configured input mode; nothing in the catalog
 *   says so, and it is never inferred from a syncword.
 *
 * Object-level timing agrees with what the library actually emits for a CMAF
 * object (`media_object.c:280-310`): the object's composition offset is the
 * FIRST sample's, and its presentation time is decode plus that offset.
 *
 * Rows are grouped by the decision they exercise; access-unit rows take an
 * INDEPENDENTLY DECLARED format so they reach the access-unit decision.
 */
class MediaContractRedTest {

    // ---- independently written fixtures ------------------------------------

    private val sps = byteArrayOf(0x67, 0x64, 0x00, 0x1f, 0xac.toByte(), 0xd9.toByte(), 0x40, 0x50, 0x05, 0xbb.toByte())
    private val pps = byteArrayOf(0x68, 0xeb.toByte(), 0xe3.toByte(), 0xcb.toByte())

    /** The legacy 25-byte record: header, one SPS, one PPS, no extension. */
    private val legacyRecord =
        byteArrayOf(
            0x01, // configurationVersion
            0x64, // AVCProfileIndication: High
            0x00, // profile_compatibility
            0x1f, // AVCLevelIndication: 31 (level 3.1)
            0xff.toByte(), // 111111 + lengthSizeMinusOne = 3: four-byte prefixes
            0xe1.toByte(), // 111 + numOfSequenceParameterSets = 1
            0x00, 0x0a,
        ) + sps + byteArrayOf(0x01, 0x00, 0x04) + pps

    /** The COMPLETE High-profile record, extension fields included. */
    private val avcRecord =
        legacyRecord + byteArrayOf(0xfd.toByte(), 0xf8.toByte(), 0xf8.toByte(), 0x00)

    /** Two length-prefixed NALs: a five-byte IDR slice and a three-byte slice. */
    private val lengthPrefixed =
        byteArrayOf(0x00, 0x00, 0x00, 0x05, 0x65, 0x01, 0x02, 0x03, 0x04) +
            byteArrayOf(0x00, 0x00, 0x00, 0x03, 0x41, 0xaa.toByte(), 0xbb.toByte())

    private val annexB =
        byteArrayOf(0x00, 0x00, 0x00, 0x01, 0x65, 0x01, 0x02, 0x03, 0x04) +
            byteArrayOf(0x00, 0x00, 0x00, 0x01, 0x41, 0xaa.toByte(), 0xbb.toByte())

    private val firstNalPrefixed = lengthPrefixed.copyOfRange(0, 9)
    private val firstNalAnnexB = annexB.copyOfRange(0, 9)
    private val secondNalAnnexB = annexB.copyOfRange(9, annexB.size)

    private val ascBytes = byteArrayOf(0x12, 0x10)
    private val aacPayload = byteArrayOf(0x21, 0x22, 0x23, 0x24, 0x25)

    /** `aac_frame_length` is 12: seven header bytes plus five payload bytes. */
    private val adtsFrame =
        byteArrayOf(0xff.toByte(), 0xf1.toByte(), 0x50, 0x80.toByte(), 0x01, 0x9f.toByte(), 0xfc.toByte()) + aacPayload

    /** trun sample flags. */
    private val nonSyncFlags = 0x01010000u
    private val syncFlags = 0x02000000u // depends_on = 2: depends on nothing

    private val capture = UnixMicros(1_746_104_600_000_000uL)

    // ---- independently declared formats ------------------------------------

    private val avcConfig =
        AvcConfig(
            sequenceParameterSets = listOf(sps),
            pictureParameterSets = listOf(pps),
            nalLengthSize = 4,
            profile = 0x64,
            constraints = 0x00,
            level = 0x1f,
        )

    private fun videoFormat(
        framing: FramingPolicy = FramingPolicy.StrictLengthPrefixed,
        packaging: PackagingKind = PackagingKind.Cmaf,
        timescale: UInt = 90_000u,
    ) = FormatOutcome.Video(
        mime = "video/avc",
        width = 1280,
        height = 720,
        csd = listOf(MediaContract.START_CODE + sps, MediaContract.START_CODE + pps),
        avc = avcConfig,
        packaging = packaging,
        framing = framing,
        timescale = timescale,
    )

    private fun audioFormat(
        bitstream: AacBitstream = AacBitstream.Raw,
        packaging: PackagingKind = PackagingKind.Cmaf,
        timescale: UInt = 44_100u,
    ) = FormatOutcome.Audio(
        mime = "audio/mp4a-latm",
        csd = listOf(ascBytes),
        aac = AacConfig(audioObjectType = 2, samplingFrequency = 44_100, channelCount = 2),
        packaging = packaging,
        bitstream = bitstream,
        timescale = timescale,
    )

    private fun sample(size: Int, duration: Int = 3_000, cts: Int = 0, flags: UInt = syncFlags) =
        SampleFacts(size.toUInt(), duration.toUInt(), cts, flags)

    /**
     * A coherent single-sample CMAF object: the first sample's 3000-tick
     * composition offset is 33333 us, which is exactly the object's offset and
     * the difference between its decode and presentation times.
     */
    private fun obj(
        payload: ByteArray,
        status: ObjectStatus = ObjectStatus.Normal,
        keyframe: Boolean = true,
        presentationUs: Long = 1_500_000,
        decodeUs: Long = 1_466_667,
        offsetUs: Long = 33_333,
        captureTime: UnixMicros? = capture,
        samples: List<SampleFacts> = listOf(sample(payload.size, cts = 3_000)),
    ) = ObjectFacts(
        payload = payload,
        status = status,
        keyframe = keyframe,
        presentationTime = MediaMicros(presentationUs.toULong()),
        decodeTime = MediaMicros(decodeUs.toULong()),
        compositionOffset = MediaOffsetMicros(offsetUs),
        captureTime = captureTime,
        samples = samples,
    )

    /**
     * A RAW object as the parser really reports one: with a timestamp, it
     * drives BOTH decode and presentation (`media_object.c:218-237`); without,
     * every timing field stays zero.
     */
    private fun rawObj(payload: ByteArray, captureTime: UnixMicros? = capture) =
        ObjectFacts(
            payload = payload,
            status = ObjectStatus.Normal,
            keyframe = true,
            presentationTime = MediaMicros(captureTime?.value ?: 0uL),
            decodeTime = MediaMicros(captureTime?.value ?: 0uL),
            compositionOffset = MediaOffsetMicros(0),
            captureTime = captureTime,
            samples = emptyList(),
        )

    private val videoTrackFacts =
        TrackFacts(
            mediaType = MediaType.Video,
            packaging = PackagingKind.Cmaf,
            codec = "avc1.64001f",
            codecConfig = avcRecord,
            timescale = 90_000u,
            width = 1280u,
            height = 720u,
        )

    private val audioTrackFacts =
        TrackFacts(
            mediaType = MediaType.Audio,
            packaging = PackagingKind.Cmaf,
            codec = "mp4a.40.2",
            codecConfig = ascBytes,
            timescale = 44_100u,
            samplerate = 44_100u,
            channelCount = 2u,
        )

    // ---- m: the configuration record ---------------------------------------

    @Test
    fun m1_theRecordYieldsItsExactParameterSetsAndPrefixWidth() {
        val config = Avc.parseConfig(avcRecord)
        assertContentEquals(sps, config.sequenceParameterSets.single(), "the SPS must be the record's bytes")
        assertContentEquals(pps, config.pictureParameterSets.single(), "the PPS must be the record's bytes")
        assertEquals(4, config.nalLengthSize, "lengthSizeMinusOne is 3, so prefixes are four bytes")
        assertEquals(0x64, config.profile)
        assertEquals(0x00, config.constraints)
        assertEquals(0x1f, config.level)
    }

    @Test
    fun m2_theSampleEntryIsAnInputBecauseTheRecordDoesNotCarryIt() {
        assertEquals("avc1.64001f", Avc.codecString(avcConfig, "avc1"))
        assertEquals("avc3.64001f", Avc.codecString(avcConfig, "avc3"), "the same record yields either entry")
    }

    @Test
    fun m3_theWriterProducesTheCompleteHighProfileRecord() {
        // The 29-byte record the project's own builder produces from these
        // parameter sets. This is a record WRITER/READER compatibility fixture:
        // it is not yet evidence of a publisher CSD-collection-to-player path.
        val built = Avc.buildConfig(listOf(sps), listOf(pps), nalLengthSize = 4)
        assertEquals(29, built.size, "a High-profile record carries the chroma/bit-depth extension")
        assertContentEquals(avcRecord, built)
        assertContentEquals(byteArrayOf(0xfd.toByte(), 0xf8.toByte(), 0xf8.toByte(), 0x00), built.copyOfRange(25, 29))
        val reparsed = Avc.parseConfig(built)
        assertContentEquals(sps, reparsed.sequenceParameterSets.single())
        assertContentEquals(pps, reparsed.pictureParameterSets.single())
        assertEquals("avc1.64001f", Avc.codecString(reparsed, "avc1"), "the publisher declares avc1 out of band")
    }

    @Test
    fun m4_aLegacyRecordWithoutTheExtensionIsStillReadable() {
        // Reader tolerance, kept separate from construction correctness.
        val config = Avc.parseConfig(legacyRecord)
        assertContentEquals(sps, config.sequenceParameterSets.single())
        assertEquals(4, config.nalLengthSize)
    }

    @Test
    fun m5_aTruncatedRecordIsRefusedRatherThanRead() {
        val failure = runCatching { Avc.parseConfig(avcRecord.copyOf(5)) }.exceptionOrNull()
        assertIs<IllegalArgumentException>(failure, "a record that ends inside its parameter sets is unusable")
    }

    @Test
    fun m6_theTrunFlagsDecideEachSamplesSyncClassification() {
        assertFalse(Cmaf.sampleIsSync(nonSyncFlags), "sample_is_non_sync_sample wins")
        assertTrue(Cmaf.sampleIsSync(syncFlags), "depends_on = 2 depends on nothing")
        assertTrue(Cmaf.sampleIsSync(0u), "flags of zero is a sync sample")
        assertFalse(Cmaf.sampleIsSync(0x01000000u), "depends_on = 1 depends on others")
    }

    @Test
    fun m7_locDisambiguatesAStartCodeFromAOneByteLength() {
        // LOC-01 2.1.3: a leading four-byte value of 1 is the start code. The
        // NALs here are multi-byte, so the two readings really do differ: as a
        // length prefix this would claim a one-byte NAL and leave a tail.
        assertEquals(VideoFraming.AnnexB, Avc.framingOf(annexB, avcConfig))
        assertEquals(VideoFraming.LengthPrefixed, Avc.framingOf(lengthPrefixed, avcConfig))
    }

    // ---- f: the format decision --------------------------------------------

    @Test
    fun f1_anAvcTrackConfiguresTheCompleteDeclaredVideoFormat() {
        val video = assertIs<FormatOutcome.Video>(DecoderFormats.of(videoTrackFacts))
        assertEquals("video/avc", video.mime)
        assertEquals(1280, video.width)
        assertEquals(720, video.height)
        assertEquals(2, video.csd.size, "csd-0 and csd-1, in that order")
        assertContentEquals(MediaContract.START_CODE + sps, video.csd[0], "csd-0 is the start-code framed SPS")
        assertContentEquals(MediaContract.START_CODE + pps, video.csd[1], "csd-1 is the start-code framed PPS")
        assertEquals(FramingPolicy.StrictLengthPrefixed, video.framing, "a CMAF sample table is the container's framing")
        assertEquals(PackagingKind.Cmaf, video.packaging, "the packaging it was decided from")
        assertEquals(90_000u, video.timescale, "the track's own tick rate, carried to the split")
        assertEquals(4, video.avc.nalLengthSize)
        assertEquals(0x64, video.avc.profile)
        assertEquals(0x00, video.avc.constraints)
        assertEquals(0x1f, video.avc.level)
        assertContentEquals(sps, video.avc.sequenceParameterSets.single(), "by content, not by reference")
        assertContentEquals(pps, video.avc.pictureParameterSets.single())
    }

    @Test
    fun f2_anAacTrackConfiguresAudioMp4aLatmWithItsAudioSpecificConfig() {
        val audio = assertIs<FormatOutcome.Audio>(DecoderFormats.of(audioTrackFacts))
        assertEquals("audio/mp4a-latm", audio.mime)
        assertContentEquals(ascBytes, audio.csd.single(), "csd-0 is the AudioSpecificConfig itself")
        assertEquals(2, audio.aac.audioObjectType, "AAC-LC")
        assertEquals(44_100, audio.aac.samplingFrequency, "sampling frequency index 4")
        assertEquals(2, audio.aac.channelCount)
        assertEquals(AacBitstream.Raw, audio.bitstream, "the default carriage, not a guess from bytes")
        assertEquals(PackagingKind.Cmaf, audio.packaging)
        assertEquals(44_100u, audio.timescale, "the AUDIO track's own tick rate")
    }

    @Test
    fun f3_aRawTrackIsDisambiguatedPerObjectNotDeclared() {
        val video = assertIs<FormatOutcome.Video>(DecoderFormats.of(videoTrackFacts.copy(packaging = PackagingKind.Raw)))
        assertEquals(FramingPolicy.LocDisambiguated, video.framing, "LOC-01 2.1.3 decides, per payload")
        assertEquals(PackagingKind.Raw, video.packaging)
        assertEquals(90_000u, video.timescale)
        assertEquals(4, video.avc.nalLengthSize, "the record still gives the prefix width")
        assertContentEquals(sps, video.avc.sequenceParameterSets.single())
    }

    @Test
    fun f4_anUnsupportedCodecIsNamedNotGuessed() {
        val outcome = DecoderFormats.of(videoTrackFacts.copy(codec = "vp09.00.10.08", codecConfig = null))
        val unsupported = assertIs<FormatOutcome.Unsupported>(outcome, "this tranche supports AVC and AAC only")
        assertTrue(unsupported.reason.contains("vp09"), "the refusal must name the codec it refused")
    }

    @Test
    fun f5_aTrackWithNoCodecConfigurationIsRefused() {
        assertIs<FormatOutcome.Unsupported>(DecoderFormats.of(videoTrackFacts.copy(codecConfig = null)))
    }

    @Test
    fun f6_aCmafTrackWithNoTimescaleIsRefused() {
        assertIs<FormatOutcome.Unsupported>(
            DecoderFormats.of(videoTrackFacts.copy(timescale = 0u)),
            "a CMAF sample table cannot be converted without a tick rate",
        )
    }

    // ---- a: the access unit, over a declared format -------------------------

    @Test
    fun a1_aLengthPrefixedSampleIsFramedAsAnnexBWithTheRecordsPrefixWidth() {
        val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(videoFormat(), obj(lengthPrefixed)))
        assertContentEquals(annexB, unit.bytes, "each length prefix becomes the start code")
        assertTrue(unit.sync, "a sync sample is a sync unit")
    }

    @Test
    fun a2_theUnitCarriesThePresentationTimeNotTheDecodeTime() {
        val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(videoFormat(), obj(lengthPrefixed)))
        assertEquals(1_500_000L, unit.presentationTimeUs, "presentationTimeUs is the presentation time in us")
        assertNotEquals(1_466_667L, unit.presentationTimeUs, "the decode time is a different fact")
    }

    @Test
    fun a3_ofAndSplitAgreeOnAValidSingleSampleObject() {
        val one = obj(lengthPrefixed)
        val single = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(videoFormat(), one))
        val split = AccessUnits.split(videoFormat(), one)
        assertEquals(1, split.size)
        assertEquals(single, assertIs<AccessUnitOutcome.Unit>(split.single()), "one sample, one answer")
    }

    @Test
    fun a4_aStatusObjectIsNeverADecoderPayload() {
        val status =
            obj(ByteArray(0), status = ObjectStatus.EndOfGroup, keyframe = false, samples = emptyList())
        assertEquals(AccessUnitOutcome.StatusOnly, AccessUnits.of(videoFormat(), status), "it carries no media")
        assertEquals(listOf(AccessUnitOutcome.StatusOnly), AccessUnits.split(videoFormat(), status))
    }

    @Test
    fun a5_aTruncatedNalIsRejectedRatherThanPartiallyQueued() {
        val short = firstNalPrefixed.copyOf(firstNalPrefixed.size - 2)
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.of(videoFormat(), obj(short, samples = listOf(sample(short.size, cts = 3_000)))),
            "a NAL that runs past its buffer is not a unit",
        )
    }

    @Test
    fun a6_locDisambiguationDecidesRawFramingInBothDirections() {
        val raw = videoFormat(framing = FramingPolicy.LocDisambiguated, packaging = PackagingKind.Raw)
        val converted = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(raw, rawObj(lengthPrefixed)))
        assertContentEquals(annexB, converted.bytes, "length-prefixed RAW carriage converts")
        val passed = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(raw, rawObj(annexB)))
        assertContentEquals(annexB, passed.bytes, "already Annex-B: LOC-01 2.1.3, no second conversion")
    }

    @Test
    fun a7_aStrictCmafObjectIsNeverReadAsStartCodes() {
        // The container's contract, not LOC's: an Annex-B payload on a CMAF
        // track contradicts its sample table rather than being reinterpreted.
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.of(videoFormat(), obj(annexB, samples = listOf(sample(annexB.size, cts = 3_000)))),
            "a CMAF sample is length-prefixed by the configuration record",
        )
    }

    @Test
    fun a8_adtsIsConvertedOnlyForAnExplicitlyConfiguredAdtsSource() {
        val declared = audioFormat(bitstream = AacBitstream.Adts, packaging = PackagingKind.Raw)
        val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(declared, rawObj(adtsFrame)))
        assertContentEquals(aacPayload, unit.bytes, "the seven-byte ADTS header is not part of the access unit")

        val raw = audioFormat(bitstream = AacBitstream.Raw, packaging = PackagingKind.Raw)
        val untouched = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(raw, rawObj(adtsFrame)))
        assertContentEquals(adtsFrame, untouched.bytes, "no syncword guess on a raw-configured source")
    }

    @Test
    fun a9_anAdtsFrameWhoseDeclaredLengthDisagreesIsRefused() {
        val wrong = adtsFrame.copyOf()
        // byte 5 = 0x3f makes aac_frame_length 9, against an actual 12 bytes.
        wrong[5] = 0x3f
        val declared = audioFormat(bitstream = AacBitstream.Adts, packaging = PackagingKind.Raw)
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.of(declared, rawObj(wrong)),
            "the header's own length is checked, not assumed",
        )
    }

    @Test
    fun a10_aMultiSampleFragmentIsNotOneAccessUnit() {
        val table = listOf(sample(9), sample(7))
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.of(videoFormat(), obj(lengthPrefixed, presentationUs = 1_000_000, decodeUs = 1_000_000, offsetUs = 0, samples = table)),
            "queueing a concatenation of frames under one timestamp is not a decode",
        )
    }

    // ---- s: the sample-table split -----------------------------------------

    @Test
    fun s1_fourSamplesAdvanceCumulativelyNotBySummedTruncations() {
        // Multi-byte NALs, so a start-code reading and a length reading differ.
        val nal = byteArrayOf(0x00, 0x00, 0x00, 0x03, 0x41, 0xaa.toByte(), 0xbb.toByte())
        val payload = nal + nal + nal + nal
        val table = List(4) { sample(size = 7, duration = 3_000, cts = 0, flags = syncFlags) }
        val units =
            AccessUnits.split(
                videoFormat(),
                obj(payload, presentationUs = 0, decodeUs = 0, offsetUs = 0, samples = table),
            )
        assertEquals(4, units.size, "the sample table says four samples")
        val times = units.map { assertIs<AccessUnitOutcome.Unit>(it).presentationTimeUs }
        assertEquals(listOf(0L, 33_333L, 66_666L, 100_000L), times, "cumulative ticks, converted once")
        assertNotEquals(99_999L, times[3], "not three individually truncated steps")
    }

    @Test
    fun s2_theSplitPinsTheCompleteOrderedInventory() {
        val table =
            listOf(
                sample(size = 9, duration = 3_000, cts = 0, flags = syncFlags),
                sample(size = 7, duration = 3_000, cts = 3_000, flags = nonSyncFlags),
            )
        val units =
            AccessUnits.split(
                videoFormat(),
                obj(lengthPrefixed, presentationUs = 1_000_000, decodeUs = 1_000_000, offsetUs = 0, samples = table),
            )
        assertEquals(2, units.size, "exactly two units, no more")
        val first = assertIs<AccessUnitOutcome.Unit>(units[0])
        val second = assertIs<AccessUnitOutcome.Unit>(units[1])
        assertContentEquals(firstNalAnnexB, first.bytes, "the first nine mdat bytes, framed")
        assertContentEquals(secondNalAnnexB, second.bytes, "the remaining seven, framed")
        assertEquals(1_000_000L, first.presentationTimeUs)
        assertEquals(1_066_666L, second.presentationTimeUs, "one 3000-tick step plus its own 3000-tick offset")
        assertTrue(first.sync, "the first sample's own flags")
        assertFalse(second.sync, "the second sample's own flags, not the object's keyframe fact")
    }

    @Test
    fun s3_onlyTheSecondSamplesSyncClassificationDiffers() {
        fun table(secondFlags: UInt) =
            listOf(sample(9, flags = syncFlags), sample(7, flags = secondFlags))
        fun split(secondFlags: UInt) =
            AccessUnits.split(
                videoFormat(),
                obj(lengthPrefixed, presentationUs = 1_000_000, decodeUs = 1_000_000, offsetUs = 0, samples = table(secondFlags)),
            )
        val aSecond = assertIs<AccessUnitOutcome.Unit>(split(syncFlags)[1])
        val bSecond = assertIs<AccessUnitOutcome.Unit>(split(nonSyncFlags)[1])
        assertContentEquals(aSecond.bytes, bSecond.bytes, "the bytes and times are identical")
        assertEquals(aSecond.presentationTimeUs, bSecond.presentationTimeUs)
        assertNotEquals(aSecond.sync, bSecond.sync, "only the sync classification differs")
    }

    @Test
    fun s4_aCmafAudioObjectSplitsOnItsOwnTrackTimescale() {
        // 1024 ticks at 44100 Hz is 23219 us after one conversion.
        val payload = aacPayload + aacPayload
        val table = listOf(sample(5, duration = 1_024, flags = 0u), sample(5, duration = 1_024, flags = 0u))
        val units =
            AccessUnits.split(
                audioFormat(),
                obj(payload, presentationUs = 0, decodeUs = 0, offsetUs = 0, samples = table),
            )
        assertEquals(2, units.size)
        assertEquals(0L, assertIs<AccessUnitOutcome.Unit>(units[0]).presentationTimeUs)
        assertEquals(23_219L, assertIs<AccessUnitOutcome.Unit>(units[1]).presentationTimeUs, "1024 ticks at 44100 Hz")
    }

    @Test
    fun s5_oneBadSampleRejectsTheWholeObjectWithNoAcceptedPrefix() {
        val table = listOf(sample(9), sample(99)) // the second runs past the payload
        val units =
            AccessUnits.split(
                videoFormat(),
                obj(lengthPrefixed, presentationUs = 1_000_000, decodeUs = 1_000_000, offsetUs = 0, samples = table),
            )
        assertEquals(1, units.size, "a bad tail is not a partial success")
        assertIs<AccessUnitOutcome.Rejected>(units.single(), "and the accepted prefix is never exposed")
    }

    @Test
    fun s6_aLargeButRepresentableTickValueConvertsRatherThanBeingRefused() {
        // 3,000,000,000 ticks at 1 MHz is 3,000,000,000 us: large, and valid.
        val big = SampleFacts(sizeBytes = 9u, durationTicks = 3_000_000_000u, compositionOffsetTicks = 0, flags = syncFlags)
        val units =
            AccessUnits.split(
                videoFormat(timescale = 1_000_000u),
                obj(lengthPrefixed, presentationUs = 0, decodeUs = 0, offsetUs = 0, samples = listOf(big, sample(7))),
            )
        assertEquals(2, units.size)
        assertEquals(
            3_000_000_000L,
            assertIs<AccessUnitOutcome.Unit>(units[1]).presentationTimeUs,
            "a large UInt tick count is not an overflow",
        )
    }

    @Test
    fun s7_boundsAreCheckedBeforeAnyArithmetic() {
        val huge = SampleFacts(sizeBytes = UInt.MAX_VALUE, durationTicks = 3_000u, compositionOffsetTicks = 0, flags = 0u)
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.split(videoFormat(), obj(lengthPrefixed, presentationUs = 0, decodeUs = 0, offsetUs = 0, samples = listOf(huge))).single(),
            "a sample size beyond the payload is refused, not narrowed",
        )
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.split(videoFormat(timescale = 0u), obj(lengthPrefixed, presentationUs = 0, decodeUs = 0, offsetUs = 0)).single(),
            "a zero timescale has no conversion",
        )
    }

    @Test
    fun s8_aBaseTimeBeyondLongIsRefusedAndSoIsASumThatCrossesIt() {
        // The public decode time is an unsigned 64-bit value; the platform
        // arithmetic is signed, so a base above Long.MAX has no representation.
        val beyondLong =
            ObjectFacts(
                payload = firstNalPrefixed,
                status = ObjectStatus.Normal,
                keyframe = true,
                presentationTime = MediaMicros(ULong.MAX_VALUE - 10uL),
                decodeTime = MediaMicros(ULong.MAX_VALUE - 10uL),
                compositionOffset = MediaOffsetMicros(0),
                captureTime = capture,
                samples = listOf(sample(9, cts = 0)),
            )
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.split(videoFormat(), beyondLong).single(),
            "an unsigned time past Long.MAX_VALUE is refused, not wrapped",
        )

        // A valid first sample, then an advance that crosses Long.MAX_VALUE.
        val nearMax = (Long.MAX_VALUE - 1_000L).toULong()
        val crossing =
            ObjectFacts(
                payload = lengthPrefixed,
                status = ObjectStatus.Normal,
                keyframe = true,
                presentationTime = MediaMicros(nearMax),
                decodeTime = MediaMicros(nearMax),
                compositionOffset = MediaOffsetMicros(0),
                captureTime = capture,
                samples =
                    listOf(
                        sample(9, duration = 90_000, cts = 0),
                        sample(7, duration = 3_000, cts = 0),
                    ),
            )
        val units = AccessUnits.split(videoFormat(), crossing)
        assertEquals(1, units.size, "the whole object is refused, not its prefix")
        assertIs<AccessUnitOutcome.Rejected>(units.single(), "the second sample's time crosses Long.MAX_VALUE")
    }

    @Test
    fun s9_aNegativeCompositionOffsetStaysSigned() {
        val table = listOf(sample(9, duration = 3_000, cts = -3_000, flags = syncFlags))
        val units =
            AccessUnits.split(
                videoFormat(),
                obj(firstNalPrefixed, presentationUs = 966_667, decodeUs = 1_000_000, offsetUs = -33_333, samples = table),
            )
        assertEquals(966_667L, assertIs<AccessUnitOutcome.Unit>(units.single()).presentationTimeUs, "-3000 ticks is earlier")
    }

    @Test
    fun s10_anIntMinCompositionOffsetIsWidenedBeforeItIsNegated() {
        // Int.MIN_VALUE has no positive Int counterpart: negating it before
        // widening wraps back to itself. Timescale 1 MHz, so ticks are us.
        val table = listOf(sample(9, duration = 3_000, cts = Int.MIN_VALUE, flags = syncFlags))
        val one =
            obj(
                firstNalPrefixed,
                presentationUs = 852_516_352,
                decodeUs = 3_000_000_000,
                offsetUs = -2_147_483_648,
                samples = table,
            )
        val format = videoFormat(timescale = 1_000_000u)
        val single = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(format, one))
        assertEquals(852_516_352L, single.presentationTimeUs, "3,000,000,000 - 2,147,483,648")
        val split = AccessUnits.split(format, one)
        assertEquals(1, split.size)
        assertEquals(single, assertIs<AccessUnitOutcome.Unit>(split.single()), "of() and split() agree")
    }

    // ---- c: the playback clock (the ruled RAW policy) -----------------------

    @Test
    fun c1_theOriginIsTheFirstRECEIVEDCaptureInstantAndNeverMoves() {
        val clock = PlaybackClock()
        assertNull(clock.originUs, "no origin before the first capture instant")
        assertEquals(PlaybackTiming.At(0L), clock.timeOf(rawObj(annexB, captureTime = capture)))
        val later = UnixMicros(capture.value + 33_333uL)
        assertEquals(PlaybackTiming.At(33_333L), clock.timeOf(rawObj(annexB, captureTime = later)))
        assertEquals(capture, clock.originUs, "one relationship, kept for the whole broadcast")
    }

    @Test
    fun c2_anEarlierCaptureArrivingLaterIsASignedOffsetNotAFailure() {
        // Ordinary interleaving: the first RECEIVED instant need not be the
        // earliest captured one. Rebasing, clamping or wrapping would move an
        // already-placed track; refusing would call legitimate order a fault.
        val clock = PlaybackClock()
        assertEquals(PlaybackTiming.At(0L), clock.timeOf(rawObj(annexB, captureTime = capture)))
        val earlier = UnixMicros(capture.value - 10_000uL)
        assertEquals(
            PlaybackTiming.At(-10_000L),
            clock.timeOf(rawObj(annexB, captureTime = earlier)),
            "a signed offset on the one relationship",
        )
        assertEquals(capture, clock.originUs, "and the origin is unchanged")
    }

    @Test
    fun c3_anObjectWithNoCaptureTimestampIsRefusedAndChangesNothing() {
        val fresh = PlaybackClock()
        val first = assertIs<PlaybackTiming.Refused>(
            fresh.timeOf(rawObj(annexB, captureTime = null)),
            "the sample's timed policy is stricter than the parser, which accepts it",
        )
        assertTrue(first.reason.isNotEmpty())
        assertNull(fresh.originUs, "a refusal before the first object leaves the clock unset")

        val running = PlaybackClock()
        assertEquals(PlaybackTiming.At(0L), running.timeOf(rawObj(annexB, captureTime = capture)))
        assertIs<PlaybackTiming.Refused>(running.timeOf(rawObj(annexB, captureTime = null)))
        assertEquals(capture, running.originUs, "a refusal leaves an established origin unchanged")
    }

    @Test
    fun c4_aZeroCaptureInstantIsAValueNotAnAbsence() {
        val clock = PlaybackClock()
        assertEquals(
            PlaybackTiming.At(0L),
            clock.timeOf(rawObj(annexB, captureTime = UnixMicros(0uL))),
            "presence is the nullable fact, never a magnitude test",
        )
        assertEquals(UnixMicros(0uL), clock.originUs)
    }

    @Test
    fun c5_theDIFFERENCEIsWhatMustBeRepresentableNotTheAbsoluteInstants() {
        // Two epoch instants above Long.MAX_VALUE, 250 ms apart: narrowing
        // either one first would refuse a perfectly representable separation.
        val high = UnixMicros(Long.MAX_VALUE.toULong() + 1_000uL)
        val clock = PlaybackClock()
        assertEquals(PlaybackTiming.At(0L), clock.timeOf(rawObj(annexB, captureTime = high)))
        assertEquals(
            PlaybackTiming.At(250_000L),
            clock.timeOf(rawObj(annexB, captureTime = UnixMicros(high.value + 250_000uL))),
            "the difference is small even though both instants are not",
        )
        assertEquals(high, clock.originUs)
    }

    @Test
    fun c6_aTrulyUnrepresentableDifferenceIsRefusedWithoutMovingTheOrigin() {
        val clock = PlaybackClock()
        assertEquals(PlaybackTiming.At(0L), clock.timeOf(rawObj(annexB, captureTime = UnixMicros(0uL))))
        assertIs<PlaybackTiming.Refused>(
            clock.timeOf(rawObj(annexB, captureTime = UnixMicros(ULong.MAX_VALUE))),
            "this separation has no signed 64-bit representation",
        )
        assertEquals(UnixMicros(0uL), clock.originUs, "and the refusal changed nothing")
    }

    // ---- x: the mapping from the public parts ------------------------------

    @Test
    fun x1_theVideoTrackMappingCarriesEveryCatalogFieldItNeeds() {
        val description =
            TrackDescription(
                name = MediaBytes("video0".encodeToByteArray()),
                codec = MediaBytes("avc1.64001f".encodeToByteArray()),
                mediaType = MediaType.Video,
                packagingKind = PackagingKind.Cmaf,
                timescale = 90_000u,
                initData = MediaBytes(avcRecord),
                catalogWidth = 1280u,
                catalogHeight = 720u,
            )
        assertEquals(videoTrackFacts, MediaFacts.ofTrack(description), "every field the format decision reads")
    }

    @Test
    fun x2_theAudioTrackMappingCarriesItsSamplerateAndChannelCount() {
        val description =
            TrackDescription(
                name = MediaBytes("audio0".encodeToByteArray()),
                codec = MediaBytes("mp4a.40.2".encodeToByteArray()),
                mediaType = MediaType.Audio,
                packagingKind = PackagingKind.Cmaf,
                timescale = 44_100u,
                initData = MediaBytes(ascBytes),
                catalogSamplerate = 44_100u,
                channelConfig = MediaBytes("2".encodeToByteArray()),
            )
        assertEquals(audioTrackFacts, MediaFacts.ofTrack(description), "dropping either would pass a video-only oracle")
    }

    @Test
    fun x3_theObjectMappingProducesTheWholeDeclaredImage() {
        val fragment = byteArrayOf(0x66, 0x72, 0x65, 0x65) + lengthPrefixed + byteArrayOf(0x00)
        val payload =
            MediaPayload.Cmaf(
                fragment = MediaBytes(fragment),
                mdat = ByteRange(offset = 4, length = lengthPrefixed.size),
                samples =
                    listOf(
                        CmafSample(durationTicks = 3_000u, sizeBytes = 9u, flags = syncFlags, compositionOffsetTicks = 3_000),
                        CmafSample(durationTicks = 3_000u, sizeBytes = 7u, flags = nonSyncFlags, compositionOffsetTicks = 6_000),
                    ),
            )
        // Distinct, coherent timing: the first sample's 3000-tick offset is
        // 33333 us, so DTS, offset and PTS are three different values and a
        // mapper that copied DTS into PTS, or zeroed the offset, would fail.
        val expected =
            ObjectFacts(
                payload = lengthPrefixed,
                status = ObjectStatus.Normal,
                keyframe = true,
                presentationTime = MediaMicros(1_033_333uL),
                decodeTime = MediaMicros(1_000_000uL),
                compositionOffset = MediaOffsetMicros(33_333),
                captureTime = capture,
                samples =
                    listOf(
                        SampleFacts(9u, 3_000u, 3_000, syncFlags),
                        SampleFacts(7u, 3_000u, 6_000, nonSyncFlags),
                    ),
            )
        assertEquals(
            expected,
            MediaFacts.ofObject(
                payload = payload,
                status = ObjectStatus.Normal,
                keyframe = true,
                presentationTime = MediaMicros(1_033_333uL),
                decodeTime = MediaMicros(1_000_000uL),
                compositionOffset = MediaOffsetMicros(33_333),
                captureTime = capture,
            ),
            "the whole image: mdat span, every sample field, status, keyframe, times and capture presence",
        )
        assertNotEquals(
            expected.presentationTime.value,
            expected.decodeTime.value,
            "the fixture itself distinguishes the two times",
        )
    }

    @Test
    fun x4_aRawObjectMapsItsPayloadAndKeepsCapturePresence() {
        val timed =
            MediaFacts.ofObject(
                payload = MediaPayload.Raw(MediaBytes(annexB)),
                status = ObjectStatus.Normal,
                keyframe = true,
                presentationTime = MediaMicros(capture.value),
                decodeTime = MediaMicros(capture.value),
                compositionOffset = MediaOffsetMicros(0),
                captureTime = capture,
            )
        assertEquals(rawObj(annexB), timed, "the whole declared image")

        val untimed =
            MediaFacts.ofObject(
                payload = MediaPayload.Raw(MediaBytes(annexB)),
                status = ObjectStatus.Normal,
                keyframe = true,
                presentationTime = MediaMicros(0uL),
                decodeTime = MediaMicros(0uL),
                compositionOffset = MediaOffsetMicros(0),
                captureTime = null,
            )
        assertEquals(rawObj(annexB, captureTime = null), untimed, "the whole untimed image, as the parser reports it")
        assertNull(untimed.captureTime, "absence survives the mapping and is not a zero")
    }

    @Test
    fun x5_aStatusObjectMapsToALegalEmptyImage() {
        val payload = MediaPayload.Cmaf(MediaBytes(ByteArray(0)), ByteRange(0, 0), emptyList())
        val facts =
            MediaFacts.ofObject(
                payload = payload,
                status = ObjectStatus.EndOfGroup,
                keyframe = false,
                presentationTime = MediaMicros(0uL),
                decodeTime = MediaMicros(0uL),
                compositionOffset = MediaOffsetMicros(0),
                captureTime = null,
            )
        val expected =
            ObjectFacts(
                payload = ByteArray(0),
                status = ObjectStatus.EndOfGroup,
                keyframe = false,
                presentationTime = MediaMicros(0uL),
                decodeTime = MediaMicros(0uL),
                compositionOffset = MediaOffsetMicros(0),
                captureTime = null,
                samples = emptyList(),
            )
        assertEquals(expected, facts, "the whole legal status image: no media, no sample table, nothing invented")
    }

    // ---- v: validation boundaries (1766 F2/F3/F4) ---------------------------

    @Test
    fun v1_aRecordWithNoParameterSetsIsNamedUnsupportedNotThrown() {
        // 111 + 0 SPS, then 0 PPS: structurally readable, useless as a format.
        // Zero SPS entries, but a well-formed PPS after them, so the refusal
        // has to come from the SPS count and not from the PPS count.
        val empty = byteArrayOf(0x01, 0x64, 0x00, 0x1f, 0xff.toByte(), 0xe0.toByte(), 0x01, 0x00, 0x04) + pps
        assertIs<FormatOutcome.Unsupported>(
            DecoderFormats.of(videoTrackFacts.copy(codecConfig = empty)),
            "a record with no SPS must be refused by name, never by an exception",
        )
        val noPps = byteArrayOf(0x01, 0x64, 0x00, 0x1f, 0xff.toByte(), 0xe1.toByte(), 0x00, 0x0a) + sps + byteArrayOf(0x00)
        assertIs<FormatOutcome.Unsupported>(DecoderFormats.of(videoTrackFacts.copy(codecConfig = noPps)))
    }

    @Test
    fun v2_aPartialTrailingExtensionIsMalformed() {
        val partial = avcRecord.copyOf(avcRecord.size - 1)
        assertIs<FormatOutcome.Unsupported>(
            DecoderFormats.of(videoTrackFacts.copy(codecConfig = partial)),
            "a High-profile record that stops inside its extension is malformed",
        )
    }

    @Test
    fun v3_theWriterRefusesCountsItsFieldsCannotHold() {
        assertFailsWith<IllegalArgumentException>("the SPS count field is five bits") {
            Avc.buildConfig(List(32) { sps }, listOf(pps), nalLengthSize = 4)
        }
        assertFailsWith<IllegalArgumentException>("the PPS count field is eight bits") {
            Avc.buildConfig(listOf(sps), List(256) { pps }, nalLengthSize = 4)
        }
        assertFailsWith<IllegalArgumentException>("a zero-length parameter set is not a parameter set") {
            Avc.buildConfig(listOf(sps, ByteArray(0)), listOf(pps), nalLengthSize = 4)
        }
    }

    @Test
    fun v4_anExpGolombValueThatCannotBeReadIsRefused() {
        // EXACTLY 31 leading zeros before the terminating one: the shift that
        // would build the value overflows a signed Int, so the bit reader must
        // refuse rather than deliver a negative field. This row is a bit-reader
        // discriminator; it says nothing about the rest of the SPS.
        val thirtyOneZeros =
            byteArrayOf(0x67, 0x64, 0x00, 0x1f, 0x80.toByte(), 0x00, 0x00, 0x00, 0x80.toByte(), 0x00, 0x00, 0x01, 0xc0.toByte())
        assertNull(SpsFields.of(thirtyOneZeros), "a value this wide cannot be read into an Int")
        assertFailsWith<IllegalStateException>("so the writer refuses rather than emitting a wrapped field") {
            Avc.buildConfig(listOf(thirtyOneZeros), listOf(pps), nalLengthSize = 4)
        }
    }

    @Test
    fun v5_videoDimensionsMustBeUsableBeforeAFormatIsOffered() {
        assertIs<FormatOutcome.Unsupported>(
            DecoderFormats.of(videoTrackFacts.copy(width = UInt.MAX_VALUE)),
            "a width that is not a positive Int cannot configure a decoder",
        )
        assertIs<FormatOutcome.Unsupported>(
            DecoderFormats.of(videoTrackFacts.copy(height = 0u)),
            "a zero dimension is not a video format",
        )
    }

    @Test
    fun v6_everyParameterSetTheRecordCarriedReachesTheCsd() {
        val secondSps = byteArrayOf(0x67, 0x64, 0x00, 0x1f, 0x11, 0x22)
        val secondPps = byteArrayOf(0x68, 0x33, 0x44)
        val record =
            byteArrayOf(0x01, 0x64, 0x00, 0x1f, 0xff.toByte(), 0xe2.toByte(), 0x00, 0x0a) + sps +
                byteArrayOf(0x00, 0x06) + secondSps +
                byteArrayOf(0x02, 0x00, 0x04) + pps + byteArrayOf(0x00, 0x03) + secondPps
        val video = assertIs<FormatOutcome.Video>(DecoderFormats.of(videoTrackFacts.copy(codecConfig = record)))
        // MediaCodec takes AVC csd-0 as the SPS and csd-1 as the PPS, each
        // start-code prefixed; several of either are concatenated in order.
        assertEquals(2, video.csd.size)
        assertContentEquals(
            MediaContract.START_CODE + sps + MediaContract.START_CODE + secondSps,
            video.csd[0],
            "every SPS, in record order",
        )
        assertContentEquals(
            MediaContract.START_CODE + pps + MediaContract.START_CODE + secondPps,
            video.csd[1],
            "every PPS, in record order",
        )
    }

    @Test
    fun v7_theAacChannelConfigurationTableIsNotTheRawField() {
        // ISO/IEC 14496-3: configuration 7 is 7.1, which is EIGHT channels.
        val sevenPointOne = byteArrayOf(0x12, 0x38)
        val audio = assertIs<FormatOutcome.Audio>(DecoderFormats.of(audioTrackFacts.copy(codecConfig = sevenPointOne)))
        assertEquals(8, audio.aac.channelCount, "configuration 7 is 7.1: eight channels, not seven")

        val pce = byteArrayOf(0x12, 0x00) // configuration 0: the channel set is in a PCE
        assertIs<FormatOutcome.Unsupported>(
            DecoderFormats.of(audioTrackFacts.copy(codecConfig = pce)),
            "configuration 0 does not give a channel count here",
        )
        val reserved = byteArrayOf(0x12, 0x40) // configuration 8: reserved
        assertIs<FormatOutcome.Unsupported>(DecoderFormats.of(audioTrackFacts.copy(codecConfig = reserved)))
    }

    @Test
    fun v8_heAacIsNamedUnsupportedRatherThanConfiguredAtTheBaseRate() {
        // AOT 5 (SBR) with a base rate of 22050: the decoder output rate is the
        // extension rate, which this tranche does not derive. AAC-LC only.
        val heAac = byteArrayOf(0x2b, 0x88.toByte(), 0x00)
        val outcome = DecoderFormats.of(audioTrackFacts.copy(codec = "mp4a.40.5", codecConfig = heAac))
        val unsupported = assertIs<FormatOutcome.Unsupported>(outcome, "this sample supports AAC-LC only")
        assertTrue(unsupported.reason.contains("AAC-LC"), "and says so: " + unsupported.reason)
    }

    @Test
    fun v9_adtsHeaderFieldsThisSampleDependsOnAreChecked() {
        val declared = audioFormat(bitstream = AacBitstream.Adts, packaging = PackagingKind.Raw)
        fun refused(mutate: (ByteArray) -> Unit, why: String) {
            val frame = adtsFrame.copyOf()
            mutate(frame)
            assertIs<AccessUnitOutcome.Rejected>(AccessUnits.of(declared, rawObj(frame)), why)
        }
        refused({ it[0] = 0xfe.toByte() }, "the syncword is checked")
        refused({ it[1] = (it[1].toInt() or 0x06).toByte() }, "layer must be 0 for ADTS")
        refused({ it[5] = 0x3f }, "the declared frame length is checked")
        assertIs<AccessUnitOutcome.Rejected>(
            AccessUnits.of(declared, rawObj(adtsFrame.copyOf(5))),
            "a frame shorter than its own header is refused",
        )
    }

    @Test
    fun v10_aSingleSampleUsesItsDeclaredSpanNotTheWholeMdat() {
        // Native C allows trailing mdat bytes: it only checks that the declared
        // sizes fit (`media_object.c:265-278`). The access unit is the declared
        // span, and `of()` and `split()` must agree byte for byte.
        val trailing = firstNalPrefixed + byteArrayOf(0x77, 0x77, 0x77)
        val one = obj(trailing, samples = listOf(sample(firstNalPrefixed.size, cts = 3_000)))
        val single = assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(videoFormat(), one))
        assertContentEquals(firstNalAnnexB, single.bytes, "the declared sample, not the whole mdat")
        val split = assertIs<AccessUnitOutcome.Unit>(AccessUnits.split(videoFormat(), one).single())
        assertContentEquals(single.bytes, split.bytes, "by content")
        assertEquals(single.presentationTimeUs, split.presentationTimeUs)
        assertEquals(single.sync, split.sync)
    }

    @Test
    fun v11_aSampleBoundIsCheckedBeforeAnySliceIsTaken() {
        val overflowing =
            listOf(
                sample(9, cts = 0),
                SampleFacts(sizeBytes = (Int.MAX_VALUE.toUInt() - 4u), durationTicks = 3_000u, compositionOffsetTicks = 0, flags = 0u),
            )
        val units =
            AccessUnits.split(
                videoFormat(),
                obj(lengthPrefixed, presentationUs = 1_000_000, decodeUs = 1_000_000, offsetUs = 0, samples = overflowing),
            )
        assertEquals(1, units.size)
        assertIs<AccessUnitOutcome.Rejected>(units.single(), "the bound is arithmetic, not an exception from a slice")
    }

    @Test
    fun v12_theFirstSampleKeepsTheObjectsOwnKeyframeFact() {
        // Native C uses the CMAF sample flags only as a FALLBACK; an explicit
        // LOC video frame marking already decided the object's keyframe state
        // (`media_object.c:204-214,327-331`).
        val table =
            listOf(
                sample(9, cts = 0, flags = syncFlags),
                sample(7, cts = 0, flags = syncFlags),
            )
        val units =
            AccessUnits.split(
                videoFormat(),
                obj(lengthPrefixed, keyframe = false, presentationUs = 1_000_000, decodeUs = 1_000_000, offsetUs = 0, samples = table),
            )
        assertFalse(assertIs<AccessUnitOutcome.Unit>(units[0]).sync, "the object said this is not a keyframe")
        assertTrue(assertIs<AccessUnitOutcome.Unit>(units[1]).sync, "later samples still use their own flags")

        val single = obj(firstNalPrefixed, keyframe = false, samples = listOf(sample(9, cts = 3_000, flags = syncFlags)))
        assertFalse(assertIs<AccessUnitOutcome.Unit>(AccessUnits.of(videoFormat(), single)).sync)
    }

    @Test
    fun v13_aLargeTickCountWhoseQuotientFitsIsConvertedNotRefused() {
        // 4,000,000,000,000 ticks at 1,000,000 Hz is 4,000,000,000,000 us:
        // representable, even though ticks x 1,000,000 is not.
        val big = SampleFacts(sizeBytes = 9u, durationTicks = 0u, compositionOffsetTicks = 0, flags = syncFlags)
        val huge = 4_000_000_000_000L
        val one =
            ObjectFacts(
                payload = firstNalPrefixed,
                status = ObjectStatus.Normal,
                keyframe = true,
                presentationTime = MediaMicros(huge.toULong()),
                decodeTime = MediaMicros(huge.toULong()),
                compositionOffset = MediaOffsetMicros(0),
                captureTime = capture,
                samples = listOf(big),
            )
        val unit = assertIs<AccessUnitOutcome.Unit>(AccessUnits.split(videoFormat(timescale = 1_000_000u), one).single())
        assertEquals(huge, unit.presentationTimeUs, "the quotient is what has to fit, not the product")
    }

    @Test
    fun v14_theExactLongMinDeltaIsRepresentableAndAnythingBeyondIsNot() {
        val clock = PlaybackClock()
        val origin = UnixMicros(9_223_372_036_854_775_808uL) // Long.MAX_VALUE + 1
        assertEquals(PlaybackTiming.At(0L), clock.timeOf(rawObj(annexB, captureTime = origin)))
        assertEquals(
            PlaybackTiming.At(Long.MIN_VALUE),
            clock.timeOf(rawObj(annexB, captureTime = UnixMicros(0uL))),
            "a delta of exactly 2^63 before the origin is Long.MIN_VALUE",
        )
        val beyond = PlaybackClock()
        assertEquals(PlaybackTiming.At(0L), beyond.timeOf(rawObj(annexB, captureTime = UnixMicros(origin.value + 1uL))))
        assertIs<PlaybackTiming.Refused>(
            beyond.timeOf(rawObj(annexB, captureTime = UnixMicros(0uL))),
            "one microsecond further back has no representation",
        )
    }

    // ---- n: what these oracles reject --------------------------------------
    //
    // These rows mutate the CANDIDATE and require the same comparisons the rows
    // above use to refuse it. They prove the assertions discriminate; they do
    // NOT prove a live production mutation was caught.

    /**
     * A candidate conversion that leaves the FIRST length prefix in place --
     * the mistake a loop starting at the second NAL makes. It produces exactly
     * as many bytes as the correct conversion.
     */
    private fun mutantAnnexB(sample: ByteArray, nalLengthSize: Int): ByteArray {
        val out = sample.copyOf()
        var i = nalLengthSize + (sample[nalLengthSize - 1].toInt() and 0xFF)
        while (i + nalLengthSize <= out.size) {
            var length = 0
            for (b in 0 until nalLengthSize) length = (length shl 8) or (out[i + b].toInt() and 0xFF)
            for (b in 0 until nalLengthSize - 1) out[i + b] = 0
            out[i + nalLengthSize - 1] = 1
            i += nalLengthSize + length
        }
        return out
    }

    @Test
    fun n1_theA1ComparisonRejectsASameLengthMutantConversion() {
        assertEquals(lengthPrefixed.size, annexB.size, "the two framings are the same length here")
        assertFalse(lengthPrefixed.contentEquals(annexB), "so only a byte comparison can tell them apart")
        val mutant = mutantAnnexB(lengthPrefixed, nalLengthSize = 4)
        assertEquals(annexB.size, mutant.size, "the mutant produces the same number of bytes")
        assertFailsWith<AssertionError>("a1's comparison must refuse a conversion that misses the first prefix") {
            assertContentEquals(annexB, mutant, "each length prefix becomes the start code")
        }
        assertContentEquals(
            annexB.copyOfRange(9, annexB.size),
            mutant.copyOfRange(9, mutant.size),
            "and it is wrong only in the first prefix, so a suffix check would have missed it",
        )
    }

    @Test
    fun n2_theTimeOracleRefusesTheDecodeTimeAndAMillisecondScale() {
        val facts = obj(lengthPrefixed)
        assertNotEquals(facts.presentationTime.value.toLong(), facts.decodeTime.value.toLong())
        assertEquals(
            facts.presentationTime.value.toLong(),
            facts.decodeTime.value.toLong() + facts.compositionOffset.value,
            "the fixture's decode time, composition offset and presentation time are coherent",
        )
        assertEquals(
            facts.compositionOffset.value,
            3_000L * 1_000_000L / 90_000L,
            "and its offset really is its first sample's 3000 ticks",
        )
        assertNotEquals(1_500L, 1_500_000L, "a millisecond value cannot satisfy the a2 comparison")
    }

    @Test
    fun n3_theCumulativeTickConversionIsWhatS1Discriminates() {
        assertEquals(100_000L, 9_000L * 1_000_000L / 90_000L, "cumulative: three 3000-tick steps")
        assertEquals(99_999L, 3L * (3_000L * 1_000_000L / 90_000L), "summed truncations, the wrong answer")
        assertEquals(23_219L, 1_024L * 1_000_000L / 44_100L, "and the audio step s4 pins")
        assertTrue(3_000_000_000L * 1_000_000L / 1_000_000L < Long.MAX_VALUE, "s6's large value really is representable")
    }
}
