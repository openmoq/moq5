package org.openmoq.moq5.engine

import org.openmoq.moq5.CencInfo
import org.openmoq.moq5.CmafCodecKind
import org.openmoq.moq5.CmafInitInfo
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaType
import org.openmoq.moq5.PackagingKind
import org.openmoq.moq5.TrackDescription
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.TrackDescribeBytes
import org.openmoq.moq5.jni.TrackDescribeScalars

/**
 * The single decoder from the `nativeTrackDescribe` carrier to a
 * [TrackDescription] snapshot.
 *
 * Native enums map only onto the cases the C headers declare; zero is
 * "absent", and any other value FAILS CLOSED by field name rather than being
 * folded into an invented case. 64-bit scalars are reinterpreted as `ULong`
 * with all their bits. Byte fields: the name is always present; an optional
 * byte attribute with no presence bit of its own is `null` when its native
 * span is empty; attributes with a presence bit follow the bit.
 */
internal object TrackDescriptionCodec {
    fun mediaTypeOf(raw: Long): MediaType? =
        when (raw) {
            0L -> null
            1L -> MediaType.Video
            2L -> MediaType.Audio
            else -> throw UnknownNativeValueException("media type", raw)
        }

    fun packagingKindOf(raw: Long): PackagingKind? =
        when (raw) {
            0L -> null
            1L -> PackagingKind.Raw
            2L -> PackagingKind.Cmaf
            else -> throw UnknownNativeValueException("packaging kind", raw)
        }

    fun codecKindOf(raw: Long): CmafCodecKind? =
        when (raw) {
            0L -> null
            1L -> CmafCodecKind.Avc
            2L -> CmafCodecKind.Hevc
            3L -> CmafCodecKind.Av1
            4L -> CmafCodecKind.Aac
            5L -> CmafCodecKind.Opus
            else -> throw UnknownNativeValueException("cmaf codec kind", raw)
        }

    /** The array length the carrier must have for these scalars. */
    fun expectedByteFields(scalars: LongArray): Int =
        TrackDescribeBytes.FIXED + scalars[TrackDescribeScalars.DEPENDS_COUNT].toInt() + scalars[TrackDescribeScalars.CP_REF_ID_COUNT].toInt()

    fun decode(bytes: Array<ByteArray>, scalars: LongArray): TrackDescription {
        require(scalars.size >= TrackDescribeScalars.COUNT) { "describe scalars: ${scalars.size} of ${TrackDescribeScalars.COUNT}" }
        require(bytes.size == expectedByteFields(scalars)) { "describe byte fields: ${bytes.size}, expected ${expectedByteFields(scalars)}" }
        fun flag(i: Int) = scalars[i] != 0L
        fun u32(i: Int) = scalars[i].toUInt()
        fun u64(i: Int) = scalars[i].toULong()
        fun optional(i: Int): MediaBytes? = bytes[i].takeIf { it.isNotEmpty() }?.let(::MediaBytes)
        val hasInit = flag(TrackDescribeScalars.HAS_INIT)
        val init =
            if (!hasInit) {
                null
            } else {
                CmafInitInfo(
                    codecKind = codecKindOf(scalars[TrackDescribeScalars.INIT_CODEC_KIND]),
                    timescale = u32(TrackDescribeScalars.INIT_TIMESCALE),
                    width = u32(TrackDescribeScalars.INIT_WIDTH),
                    height = u32(TrackDescribeScalars.INIT_HEIGHT),
                    samplerate = u32(TrackDescribeScalars.INIT_SAMPLERATE),
                    channelCount = u32(TrackDescribeScalars.INIT_CHANNEL_COUNT),
                    codecConfig = MediaBytes(bytes[TrackDescribeBytes.CODEC_CONFIG]),
                    trackId = u32(TrackDescribeScalars.INIT_TRACK_ID),
                    cenc =
                        if (!flag(TrackDescribeScalars.HAS_CENC)) {
                            null
                        } else {
                            CencInfo(
                                scheme = MediaBytes(bytes[TrackDescribeBytes.CENC_SCHEME]),
                                defaultKid = MediaBytes(bytes[TrackDescribeBytes.CENC_DEFAULT_KID]),
                                defaultIsProtected = flag(TrackDescribeScalars.CENC_DEFAULT_IS_PROTECTED),
                                perSampleIvSize = u32(TrackDescribeScalars.CENC_IV_SIZE),
                            )
                        },
                )
            }
        val dependsCount = scalars[TrackDescribeScalars.DEPENDS_COUNT].toInt()
        val depends = List(dependsCount) { MediaBytes(bytes[TrackDescribeBytes.FIXED + it]) }
        val refIds = List(scalars[TrackDescribeScalars.CP_REF_ID_COUNT].toInt()) { MediaBytes(bytes[TrackDescribeBytes.FIXED + dependsCount + it]) }
        return TrackDescription(
            name = MediaBytes(bytes[TrackDescribeBytes.NAME]),
            packaging = optional(TrackDescribeBytes.PACKAGING),
            role = optional(TrackDescribeBytes.ROLE),
            codec = optional(TrackDescribeBytes.CODEC),
            lang = optional(TrackDescribeBytes.LANG),
            label = optional(TrackDescribeBytes.LABEL),
            mimeType = optional(TrackDescribeBytes.MIME_TYPE),
            eventType = optional(TrackDescribeBytes.EVENT_TYPE),
            channelConfig = optional(TrackDescribeBytes.CHANNEL_CONFIG),
            mediaType = mediaTypeOf(scalars[TrackDescribeScalars.MEDIA_TYPE]),
            packagingKind = packagingKindOf(scalars[TrackDescribeScalars.PACKAGING_KIND]),
            timescale = u32(TrackDescribeScalars.TIMESCALE),
            transportVersion = u32(TrackDescribeScalars.TRANSPORT_VERSION),
            init = init,
            initData = if (hasInit) MediaBytes(bytes[TrackDescribeBytes.INIT_DATA]) else null,
            catalogWidth = if (flag(TrackDescribeScalars.HAS_WIDTH)) u32(TrackDescribeScalars.CATALOG_WIDTH) else null,
            catalogHeight = if (flag(TrackDescribeScalars.HAS_HEIGHT)) u32(TrackDescribeScalars.CATALOG_HEIGHT) else null,
            catalogSamplerate = if (flag(TrackDescribeScalars.HAS_SAMPLERATE)) u32(TrackDescribeScalars.CATALOG_SAMPLERATE) else null,
            framerateMillis = if (flag(TrackDescribeScalars.HAS_FRAMERATE)) u64(TrackDescribeScalars.FRAMERATE_MILLIS) else null,
            bitrate = if (flag(TrackDescribeScalars.HAS_BITRATE)) u64(TrackDescribeScalars.BITRATE) else null,
            maxGroupSap = if (flag(TrackDescribeScalars.HAS_MAX_GRP_SAP)) u32(TrackDescribeScalars.MAX_GRP_SAP) else null,
            maxObjectSap = if (flag(TrackDescribeScalars.HAS_MAX_OBJ_SAP)) u32(TrackDescribeScalars.MAX_OBJ_SAP) else null,
            contentProtectionRefIds = refIds,
            depends = depends,
            trackDurationMs = if (flag(TrackDescribeScalars.HAS_TRACK_DURATION)) u64(TrackDescribeScalars.TRACK_DURATION_MS) else null,
            isLive = flag(TrackDescribeScalars.IS_LIVE),
        )
    }
}
