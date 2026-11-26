package org.openmoq.moq5.engine

import org.openmoq.moq5.CmafCodecKind
import org.openmoq.moq5.InitSource
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.TrackEncryption
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
 * CMAF=2, LOCMAF=4; `moq_cmaf_codec_kind_t`: UNKNOWN=0, AVC..OPUS=1..5); zero is
 * "absent" (`null`) and any other value fails closed by name. 64-bit
 * scalars are reinterpreted as `ULong`, never narrowed or signed-compared.
 * The appended fields follow the presence mask: the generation block
 * (`config_generation`, `init_source`, `init_property_type`) only when the
 * copy's stamp covered it, each encryption field of the supplement only when
 * the catalog carried it; `init_source` is an open set.
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
        assertEquals(PackagingKind.Locmaf, TrackDescriptionCodec.packagingKindOf(4L))
        assertEquals(null, TrackDescriptionCodec.packagingKindOf(0L))
        assertEquals(null, TrackDescriptionCodec.codecKindOf(0L), "CMAF UNKNOWN is absent, not a case")
        assertEquals(listOf(CmafCodecKind.Avc, CmafCodecKind.Hevc, CmafCodecKind.Av1, CmafCodecKind.Aac, CmafCodecKind.Opus), (1L..5L).map { TrackDescriptionCodec.codecKindOf(it) })
    }

    @Test
    fun `an unknown native enum value fails closed by field name`() {
        assertEquals("media type", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.mediaTypeOf(3L) }.field)
        assertEquals("packaging kind", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.packagingKindOf(3L) }.field)
        assertEquals("packaging kind", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.packagingKindOf(5L) }.field)
        assertEquals("cmaf codec kind", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.codecKindOf(6L) }.field)
        assertEquals("media type", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.mediaTypeOf(-1L) }.field)
        val bad = scalars(TrackDescribeScalars.MEDIA_TYPE to 7L)
        assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.decode(bytes(), bad) }
    }

    @Test
    fun `a LOCMAF description keeps its own packaging kind and the CMAF header facts a CMAF track carries`() {
        val d = TrackDescriptionCodec.decode(
            bytes(),
            scalars(
                TrackDescribeScalars.MEDIA_TYPE to 1L, TrackDescribeScalars.PACKAGING_KIND to 4L,
                TrackDescribeScalars.HAS_INIT to 1L, TrackDescribeScalars.INIT_CODEC_KIND to 1L,
                TrackDescribeScalars.INIT_TIMESCALE to 90_000L, TrackDescribeScalars.INIT_TRACK_ID to 1L,
            ),
        )
        assertEquals(PackagingKind.Locmaf, d.packagingKind)
        assertEquals(MediaType.Video, d.mediaType)
        assertEquals(CmafCodecKind.Avc, d.init?.codecKind)
        assertEquals(90_000u, d.init?.timescale)
        assertEquals(1u, d.init?.trackId)
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

    @Test
    fun `the generation block decodes when the stamp covered it, and every value of the open init-source set has a case`() {
        val d = TrackDescriptionCodec.decode(
            bytes(),
            scalars(
                TrackDescribeScalars.HAS to TrackDescribeScalars.HAS_GENERATION, TrackDescribeScalars.DESC_GENERATION to 0xFFFF_FFFFL,
                TrackDescribeScalars.INIT_SOURCE to 2L, TrackDescribeScalars.INIT_PROPERTY_TYPE to 0x0DL,
            ),
        )
        assertEquals(UInt.MAX_VALUE, d.configGeneration, "all 32 bits of the generation")
        assertEquals(InitSource.TrackProperty, d.initSource)
        assertEquals(0x0DuL, d.initPropertyType)
        assertEquals(
            listOf(InitSource.None, InitSource.Inline, InitSource.TrackProperty, InitSource.ObjectProperty, InitSource.Unknown(4u), InitSource.Unknown(UInt.MAX_VALUE)),
            listOf(0L, 1L, 2L, 3L, 4L, 0xFFFF_FFFFL).map {
                TrackDescriptionCodec.decode(bytes(), scalars(TrackDescribeScalars.HAS to TrackDescribeScalars.HAS_GENERATION, TrackDescribeScalars.INIT_SOURCE to it)).initSource
            },
            "an unknown source is carried as itself, never refused or folded",
        )
        val inline = TrackDescriptionCodec.decode(bytes(), scalars(TrackDescribeScalars.HAS to TrackDescribeScalars.HAS_GENERATION, TrackDescribeScalars.INIT_SOURCE to 1L))
        assertEquals(0u, inline.configGeneration, "generation zero is a present generation")
        assertEquals(null, inline.initPropertyType, "the native zero names no property")
        assertEquals(ULong.MAX_VALUE, TrackDescriptionCodec.decode(bytes(), scalars(TrackDescribeScalars.HAS to TrackDescribeScalars.HAS_GENERATION, TrackDescribeScalars.INIT_PROPERTY_TYPE to -1L)).initPropertyType, "all 64 bits")
    }

    @Test
    fun `without the presence bits the appended fields are null, never zero`() {
        val d = TrackDescriptionCodec.decode(bytes(), scalars(TrackDescribeScalars.DESC_GENERATION to 3L, TrackDescribeScalars.INIT_SOURCE to 1L, TrackDescribeScalars.INIT_PROPERTY_TYPE to 0x0FL))
        assertEquals(null, d.configGeneration)
        assertEquals(null, d.initSource)
        assertEquals(null, d.initPropertyType)
        assertEquals(null, d.encryption, "no encryption field carried")
    }

    @Test
    fun `the four encryption fields are fixed byte fields, each present exactly when its bit is set`() {
        val b = bytes()
        b[TrackDescribeBytes.ENCRYPTION_SCHEME] = "moq-secure-objects".toByteArray()
        b[TrackDescribeBytes.CIPHER_SUITE] = "aes-128-gcm-sha256".toByteArray()
        b[TrackDescribeBytes.KEY_ID] = byteArrayOf(0, 1, 2)
        b[TrackDescribeBytes.TRACK_BASE_KEY] = byteArrayOf(9, 8, 7, 6)
        val all = TrackDescribeScalars.HAS_ENCRYPTION_SCHEME or TrackDescribeScalars.HAS_CIPHER_SUITE or TrackDescribeScalars.HAS_KEY_ID or TrackDescribeScalars.HAS_TRACK_BASE_KEY
        assertEquals(
            TrackEncryption(MediaBytes("moq-secure-objects".toByteArray()), MediaBytes("aes-128-gcm-sha256".toByteArray()), MediaBytes(byteArrayOf(0, 1, 2)), MediaBytes(byteArrayOf(9, 8, 7, 6))),
            TrackDescriptionCodec.decode(b, scalars(TrackDescribeScalars.HAS to all)).encryption,
        )
        val schemeOnly = TrackDescriptionCodec.decode(b, scalars(TrackDescribeScalars.HAS to TrackDescribeScalars.HAS_ENCRYPTION_SCHEME)).encryption
        assertEquals(TrackEncryption(scheme = MediaBytes("moq-secure-objects".toByteArray())), schemeOnly, "a field without its bit is null, whatever the array holds")
        val emptyKey = TrackDescriptionCodec.decode(bytes(), scalars(TrackDescribeScalars.HAS to TrackDescribeScalars.HAS_KEY_ID)).encryption
        assertEquals(TrackEncryption(keyId = MediaBytes(ByteArray(0))), emptyKey, "a carried empty value is present, not absent")
    }

    @Test
    fun `a presence bit outside the known set fails closed by field name`() {
        for (raw in listOf(0x20L, Long.MIN_VALUE, -1L)) {
            assertEquals("describe presence", assertFailsWith<UnknownNativeValueException> { TrackDescriptionCodec.decode(bytes(), scalars(TrackDescribeScalars.HAS to raw)) }.field, "bits $raw")
        }
    }

    @Test
    fun `the expected byte-field count is the seventeen fixed fields plus depends and the ref ids, and a mismatch is refused`() {
        assertEquals(17, TrackDescriptionCodec.expectedByteFields(scalars()))
        assertEquals(20, TrackDescriptionCodec.expectedByteFields(scalars(TrackDescribeScalars.DEPENDS_COUNT to 1L, TrackDescribeScalars.CP_REF_ID_COUNT to 2L)))
        assertFailsWith<IllegalArgumentException> { TrackDescriptionCodec.decode(Array(13) { ByteArray(0) }, scalars()) }
        val d = TrackDescriptionCodec.decode(bytes("dep".toByteArray()), scalars(TrackDescribeScalars.DEPENDS_COUNT to 1L, TrackDescribeScalars.HAS to TrackDescribeScalars.HAS_KEY_ID))
        assertEquals(listOf(MediaBytes("dep".toByteArray())), d.depends, "depends start after the encryption fields")
        assertEquals(MediaBytes(ByteArray(0)), d.encryption?.keyId)
    }

    @Test
    fun `the track base key is key material -- no rendering of the encryption value or of the description prints it`() {
        val secret = byteArrayOf(0x5e, 0x77, 0x0c, 0x21)
        val e = TrackEncryption(scheme = MediaBytes("moq-secure-objects".toByteArray()), trackBaseKey = MediaBytes(secret))
        val hex = MediaBytes(secret).toString().substringAfter("hex:").substringBefore(")")
        val d = TrackDescriptionCodec.decode(bytes(), scalars()).copy(encryption = e)
        for (text in listOf(e.toString(), d.toString())) {
            assertTrue(!text.contains(hex), "no hex of the key in: $text")
            assertTrue(text.contains("trackBaseKey=<redacted>"), "presence is still visible: $text")
        }
        assertTrue(TrackEncryption().toString().contains("trackBaseKey=null"), "absence is visible too")
        assertEquals(MediaBytes(secret), e.trackBaseKey, "the value itself stays available to the application")
    }
}
