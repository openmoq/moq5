package org.openmoq.moq5.engine

import org.openmoq.moq5.CmafCodecKind
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.TrackDescribeBytes
import org.openmoq.moq5.jni.TrackDescribeScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The decoder from the `nativeTrackDescribe` carrier to a [TrackDescription]
 * snapshot. Native enums are mapped only onto the cases the C headers declare
 * (`moq_media_type_t`: VIDEO=1, AUDIO=2; `moq_media_packaging_t`: RAW=1,
 * CMAF=2; `moq_cmaf_codec_kind_t`: UNKNOWN=0, AVC..OPUS=1..5); zero is
 * "absent" (`null`) and any other value fails closed by name. 64-bit
 * scalars are reinterpreted as `ULong`, never narrowed or signed-compared.
 */
internal class TrackDescriptionCodecTest {
    private fun scalars(vararg set: Pair<Int, Long>): LongArray = LongArray(TrackDescribeScalars.COUNT).also { for ((i, v) in set) it[i] = v }
    private fun bytes(vararg extra: ByteArray): Array<ByteArray> = Array(TrackDescribeBytes.FIXED) { ByteArray(0) }.also { it[TrackDescribeBytes.NAME] = "a".toByteArray() } + extra

    @Test
    fun `known native enum values map onto the declared cases and zero is absent`() {
        assertEquals(MediaType.Video, TrackDescriptionCodec.mediaTypeOf(1L))
        assertEquals(MediaType.Audio, TrackDescriptionCodec.mediaTypeOf(2L))
        assertEquals(null, TrackDescriptionCodec.mediaTypeOf(0L))
        assertEquals(PackagingKind.Raw, TrackDescriptionCodec.packagingKindOf(1L))
        assertEquals(PackagingKind.Cmaf, TrackDescriptionCodec.packagingKindOf(2L))
        assertEquals(null, TrackDescriptionCodec.packagingKindOf(0L))
        assertEquals(null, TrackDescriptionCodec.codecKindOf(0L), "CMAF UNKNOWN is absent, not a case")
        assertEquals(listOf(CmafCodecKind.Avc, CmafCodecKind.Hevc, CmafCodecKind.Av1, CmafCodecKind.Aac, CmafCodecKind.Opus), (1L..5L).map { TrackDescriptionCodec.codecKindOf(it) })
    }

    @Test
    fun `an unknown native enum value fails closed by field name`() {
        assertEquals("media type", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.mediaTypeOf(3L) }.field)
        assertEquals("packaging kind", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.packagingKindOf(3L) }.field)
        assertEquals("cmaf codec kind", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.codecKindOf(6L) }.field)
        assertEquals("media type", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.mediaTypeOf(-1L) }.field)
        val bad = scalars(TrackDescribeScalars.MEDIA_TYPE to 7L)
        assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.decode(bytes(), bad) }
    }

    @Test
    fun `64-bit scalars decode as ULong with all bits, and absent flags decode as null`() {
        val d = TrackDescriptionCodec.decode(bytes(), scalars(TrackDescribeScalars.HAS_BITRATE to 1L, TrackDescribeScalars.BITRATE to -1L, TrackDescribeScalars.HAS_FRAMERATE to 1L, TrackDescribeScalars.FRAMERATE_MILLIS to Long.MIN_VALUE, TrackDescribeScalars.HAS_TRACK_DURATION to 0L, TrackDescribeScalars.TRACK_DURATION_MS to 5L, TrackDescribeScalars.IS_LIVE to 1L))
        assertEquals(ULong.MAX_VALUE, d.bitrate)
        assertEquals(0x8000_0000_0000_0000uL, d.framerateMillis)
        assertEquals(null, d.trackDurationMs, "a cleared has_ flag hides the slot")
        assertTrue(d.isLive)
        assertEquals(MediaBytes("a".toByteArray()), d.name)
        assertEquals(null, d.catalogWidth)
        assertEquals(null, d.init, "no init unless HAS_INIT")
    }

    @Test
    fun `catalog passthrough dimensions and init-derived dimensions decode into distinct fields`() {
        val d = TrackDescriptionCodec.decode(
            bytes(),
            scalars(
                TrackDescribeScalars.HAS_WIDTH to 1L, TrackDescribeScalars.CATALOG_WIDTH to 1280L,
                TrackDescribeScalars.HAS_INIT to 1L, TrackDescribeScalars.INIT_CODEC_KIND to 1L, TrackDescribeScalars.INIT_TIMESCALE to 90_000L,
                TrackDescribeScalars.INIT_WIDTH to 1920L, TrackDescribeScalars.INIT_HEIGHT to 1080L, TrackDescribeScalars.INIT_TRACK_ID to 1L,
            ),
        )
        assertEquals(1280u, d.catalogWidth)
        assertEquals(null, d.catalogHeight)
        assertEquals(CmafCodecKind.Avc, d.init?.codecKind)
        assertEquals(1920u, d.init?.width)
        assertEquals(1080u, d.init?.height)
        assertEquals(90_000u, d.init?.timescale)
        assertEquals(1u, d.init?.trackId)
        assertEquals(null, d.init?.cenc)
    }

    @Test
    fun `depends and content-protection ref ids come after the fixed byte fields, in order`() {
        val d = TrackDescriptionCodec.decode(bytes("v".toByteArray(), "cp1".toByteArray(), "cp2".toByteArray()), scalars(TrackDescribeScalars.DEPENDS_COUNT to 1L, TrackDescribeScalars.CP_REF_ID_COUNT to 2L))
        assertEquals(listOf(MediaBytes("v".toByteArray())), d.depends)
        assertEquals(listOf(MediaBytes("cp1".toByteArray()), MediaBytes("cp2".toByteArray())), d.contentProtectionRefIds)
    }
}
