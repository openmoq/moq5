package org.openmoq.moq5.samples.player

import org.openmoq.moq5.ByteRange
import org.openmoq.moq5.CmafSample
import org.openmoq.moq5.CmafInitInfo
import org.openmoq.moq5.CmafCodecKind
import org.openmoq.moq5.CencInfo
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
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class PlaybackReceiveTest {
    // Length-four baseline AVC record with one SPS and PPS. Framing fixture,
    // not a decodability or hardware-capability claim.
    private val avcc = byteArrayOf(1, 66, 0, 30, -1, -31, 0, 4, 0x67, 66, 0, 30, 1, 0, 2, 0x68, 0)
    private val asc = byteArrayOf(0x12, 0x10)
    private val nal = byteArrayOf(0, 0, 0, 2, 0x65, 42)
    private fun description(audio: Boolean = false, packaging: PackagingKind = PackagingKind.Raw) = TrackDescription(
        name = MediaBytes(if (audio) byteArrayOf(97) else byteArrayOf(118)),
        codec = MediaBytes((if (audio) "mp4a.40.2" else "avc1.42001e").encodeToByteArray()),
        mediaType = if (audio) MediaType.Audio else MediaType.Video,
        packagingKind = packaging, initData = MediaBytes(if (audio) asc else avcc),
        catalogWidth = 640u, catalogHeight = 480u, catalogSamplerate = 44100u,
        channelConfig = MediaBytes("2".encodeToByteArray()), timescale = 1000u,
    )
    private fun formats(packaging: PackagingKind = PackagingKind.Raw) =
        (DecoderFormats.of(MediaFacts.ofTrack(description(packaging = packaging))) as FormatOutcome.Video) to
            (DecoderFormats.of(MediaFacts.ofTrack(description(true, packaging))) as FormatOutcome.Audio)

    private fun raw(mapper: PlaybackReceiveMedia, lane: PlaybackLane, capture: ULong?, bytes: ByteArray = nal) = mapper.map(
        lane, MediaPayload.Raw(MediaBytes(bytes)), ObjectStatus.Normal, true,
        MediaMicros(capture ?: 0u), MediaMicros(capture ?: 0u), MediaOffsetMicros(0), capture?.let(::UnixMicros),
    )

    @Test fun selectionPreservesIdentityAndSkipsAbsentProtectedAndDependentTracks() {
        val dead = Any(); val encrypted = Any(); val dependent = Any(); val video = Any(); val audio = Any()
        val picked = requireNotNull(selectPlayback(listOf(
            PlaybackCandidate(dead, description(), false),
            PlaybackCandidate(encrypted, description().copy(contentProtectionRefIds = listOf(MediaBytes(byteArrayOf(1)))), true),
            PlaybackCandidate(dependent, description().copy(depends = listOf(MediaBytes(byteArrayOf(2)))), true),
            PlaybackCandidate(video, description(), true), PlaybackCandidate(audio, description(true), true),
        )))
        assertSame(video, picked.video); assertSame(audio, picked.audio)
        assertEquals("video/avc", picked.videoFormat.mime); assertEquals("audio/mp4a-latm", picked.audioFormat?.mime)
    }

    @Test fun emptyVideoInventoryWaitsButUnsupportedPresentVideoFailsByName() {
        assertNull(selectPlayback(listOf(PlaybackCandidate(Any(), description(true), true))))
        val error = assertFailsWith<PlaybackRenderRefused> {
            selectPlayback(listOf(PlaybackCandidate(Any(), description().copy(codec = MediaBytes("vp09".encodeToByteArray())), true)))
        }
        assertTrue(error.message!!.contains("video"))
    }

    @Test fun optionalAudioMustNotSilentlyTurnUnsupportedAudioIntoMutePlayback() {
        assertFailsWith<PlaybackRenderRefused> {
            selectPlayback(listOf(PlaybackCandidate(Any(), description(), true),
                PlaybackCandidate(Any(), description(true).copy(initData = null), true)))
        }
        val single = requireNotNull(selectPlayback(listOf(PlaybackCandidate(Any(), description(), true))))
        assertNull(single.audio); assertNull(single.audioFormat)
    }

    @Test fun noImplicitMixedTimeline() {
        val (v, _) = formats(); val (_, a) = formats(PackagingKind.Cmaf)
        assertFailsWith<IllegalArgumentException> { PlaybackReceiveMedia(v, a) }
        assertFailsWith<PlaybackRenderRefused> {
            selectPlayback(listOf(PlaybackCandidate(Any(), description(), true),
                PlaybackCandidate(Any(), description(true, PackagingKind.Cmaf), true)))
        }
    }

    @Test fun chooseFirstCompleteCompatiblePairNotAnUnplayableFirstVideo() {
        val raw = Any(); val video = Any(); val audio = Any()
        val selected = requireNotNull(selectPlayback(listOf(
            PlaybackCandidate(raw, description(), true),
            PlaybackCandidate(video, description(packaging = PackagingKind.Cmaf), true),
            PlaybackCandidate(audio, description(true, PackagingKind.Cmaf), true),
        )))
        assertSame(video, selected.video); assertSame(audio, selected.audio)
    }

    @Test fun encryptedSampleEntryIsNotProofOfClearMediaEvenWhenDefaultIsFalse() {
        for (protected in listOf(false, true)) {
            val encrypted = description(packaging = PackagingKind.Cmaf).copy(init = CmafInitInfo(
                CmafCodecKind.Avc, 1000u, 640u, 480u, 0u, 0u, MediaBytes(avcc), 1u,
                CencInfo(MediaBytes("cenc".encodeToByteArray()), MediaBytes(ByteArray(16)), protected, 0u),
            ))
            assertFailsWith<PlaybackRenderRefused> { selectPlayback(listOf(PlaybackCandidate(Any(), encrypted, true))) }
            val clear = Any()
            assertSame(clear, selectPlayback(listOf(PlaybackCandidate(Any(), encrypted, true),
                PlaybackCandidate(clear, description(), true)))?.video)
        }
    }

    @Test fun rawUsesOneCaptureClockWithoutNarrowingEpochOrLosingNegativeAudioOffset() {
        val (v, a) = formats(); val mapper = PlaybackReceiveMedia(v, a)
        val epoch = Long.MAX_VALUE.toULong() + 5000u
        val video = raw(mapper, PlaybackLane.Video, epoch).single()
        val audio = raw(mapper, PlaybackLane.Audio, epoch - 20u, byteArrayOf(7, 8)).single()
        val later = raw(mapper, PlaybackLane.Video, epoch + 30u).single()
        assertEquals(listOf(0L, -20L, 30L), listOf(video.logicalUs, audio.logicalUs, later.logicalUs))
        assertContentEquals(byteArrayOf(0, 0, 0, 1, 0x65, 42), video.unit.bytes)
        assertContentEquals(byteArrayOf(7, 8), audio.unit.bytes)
    }

    @Test fun missingRawCaptureRefusesButStatusNeitherNeedsTimeNorAnchorsClock() {
        val (v, a) = formats(); val mapper = PlaybackReceiveMedia(v, a)
        assertFailsWith<PlaybackRenderRefused> { raw(mapper, PlaybackLane.Video, null) }
        assertTrue(mapper.map(PlaybackLane.Video, MediaPayload.Raw(MediaBytes(byteArrayOf())), ObjectStatus.EndOfGroup,
            false, MediaMicros(0u), MediaMicros(0u), MediaOffsetMicros(0), null).isEmpty())
        assertEquals(0L, raw(mapper, PlaybackLane.Video, 700u).single().logicalUs)
    }

    @Test fun malformedRawObjectDoesNotCommitClockOrigin() {
        val (v, a) = formats(); val mapper = PlaybackReceiveMedia(v, a)
        assertFailsWith<PlaybackRenderRefused> { raw(mapper, PlaybackLane.Video, 100u, byteArrayOf(0)) }
        assertEquals(0L, raw(mapper, PlaybackLane.Video, 700u).single().logicalUs)
    }

    @Test fun cmafKeepsDecodeOrderAndSignedCompositionWithoutUsingCaptureEpoch() {
        val (v, a) = formats(PackagingKind.Cmaf); val mapper = PlaybackReceiveMedia(v, a)
        val samples = listOf(CmafSample(1u, 6u, 0x02000000u, 2), CmafSample(1u, 6u, 0x01010000u, -2))
        val result = mapper.map(PlaybackLane.Video, MediaPayload.Cmaf(MediaBytes(nal + nal), ByteRange(0, 12), samples),
            ObjectStatus.Normal, true, MediaMicros(3000u), MediaMicros(1000u), MediaOffsetMicros(2000), UnixMicros(ULong.MAX_VALUE))
        assertEquals(listOf(3000L, 0L), result.map { it.logicalUs })
        assertEquals(listOf(true, false), result.map { it.unit.sync })
    }

    @Test fun boundsAndPayloadPackagingAreCheckedBeforeMediaMapping() {
        val (v, a) = formats(); val mapper = PlaybackReceiveMedia(v, a, maxPayloadBytes = 5, maxSamples = 1)
        assertFailsWith<PlaybackRenderRefused> { raw(mapper, PlaybackLane.Video, 1u) }
        val cmaf = MediaPayload.Cmaf(MediaBytes(nal), ByteRange(0, 6), listOf(CmafSample(1u, 6u, 0u, 0)))
        assertFailsWith<PlaybackRenderRefused> { mapper.map(PlaybackLane.Video, cmaf, ObjectStatus.Normal, true,
            MediaMicros(0u), MediaMicros(0u), MediaOffsetMicros(0), UnixMicros(0u)) }
        val (cv, ca) = formats(PackagingKind.Cmaf)
        val limited = PlaybackReceiveMedia(cv, ca, maxSamples = 1)
        assertFailsWith<PlaybackRenderRefused> { limited.map(PlaybackLane.Video,
            cmaf.copy(fragment = MediaBytes(nal + nal), mdat = ByteRange(0, 12), samples = cmaf.samples + cmaf.samples), ObjectStatus.Normal, true,
            MediaMicros(0u), MediaMicros(0u), MediaOffsetMicros(0), null) }
    }

    @Test fun avcExpandedSizeIsBoundedBeforeAllocationAndAcrossTheWholeFragment() {
        val (v, _) = formats(PackagingKind.Cmaf)
        val narrow = v.copy(avc = v.avc.copy(nalLengthSize = 1))
        // Four input bytes -> ten output bytes, so this cannot fit in eight.
        assertNull(Avc.toAnnexBOrNull(byteArrayOf(1, 0x65, 1, 0x41), narrow.avc, 8))
        assertContentEquals(byteArrayOf(0, 0, 0, 1, 0x65), Avc.toAnnexBOrNull(byteArrayOf(1, 0x65), narrow.avc, 5))
        val mapper = PlaybackReceiveMedia(narrow, null, maxPayloadBytes = 8)
        assertFailsWith<PlaybackRenderRefused> { mapper.map(PlaybackLane.Video,
            MediaPayload.Cmaf(MediaBytes(byteArrayOf(1, 0x65, 1, 0x41)), ByteRange(0, 4),
                listOf(CmafSample(1u, 2u, 0u, 0), CmafSample(1u, 2u, 0u, 0))),
            ObjectStatus.Normal, true, MediaMicros(0u), MediaMicros(0u), MediaOffsetMicros(0), null) }
    }

    @Test fun manyTinyNalsAreNamedRefusalRatherThanBoxedAllocationExhaustion() {
        val (v, _) = formats(PackagingKind.Cmaf)
        val narrow = v.copy(avc = v.avc.copy(nalLengthSize = 1))
        val size = 8 * 1024 * 1024 - 1024
        val payload = MediaPayload.Cmaf(MediaBytes(ByteArray(size) { if (it % 2 == 0) 1 else 0x65 }),
            ByteRange(0, size), listOf(CmafSample(1u, size.toUInt(), 0u, 0)))
        assertFailsWith<PlaybackRenderRefused> { PlaybackReceiveMedia(narrow, null).map(PlaybackLane.Video,
            payload, ObjectStatus.Normal, true, MediaMicros(0u), MediaMicros(0u), MediaOffsetMicros(0), null) }
    }
}
