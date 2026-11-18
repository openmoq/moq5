package org.openmoq.moq5.engine

import org.openmoq.moq5.ByteRange
import org.openmoq.moq5.CmafSample
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.WireIdentity
import org.openmoq.moq5.jni.ReceiverObjectBytes
import org.openmoq.moq5.jni.ReceiverObjectScalars
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * The single decoder from the `nativeReceiverPollObject` carrier to a
 * [BackendObject]. It fails CLOSED: object status is exactly 0, 1 or 2 and
 * object packaging exactly RAW (1) or CMAF (2) -- there is no absent value
 * at the object level, unlike the track description's catalog fields --
 * and any other value throws by field name.
 *
 * A CMAF object's media rules describe its MEDIA objects: a NORMAL one has an
 * mdat span that is non-negative, inside the copied fragment (checked in 64
 * bits before any narrowing) and at least one byte, with a packed sample
 * carrier exactly `sample_count` times the stride.
 *
 * A CMAF object whose status is not NORMAL carries NO media at all. The C
 * accepts such an object with no payload and returns before parsing any media,
 * so its carrier is empty in every media field, and requiring a byte of mdat
 * there would refuse a legal object. It decodes to CMAF with an empty fragment,
 * an empty span and no samples. A status object that CLAIMS media is refused by
 * name rather than normalised into an empty one.
 *
 * Every refusal happens before a value exists. RAW media comes only from the
 * payload; CMAF media only from the mdat slice.
 *
 * The identity scalars are read only under `HAS_IDENTITY`, the bridge's word
 * that the native record covered the whole tail; without it every identity
 * scalar must be zero and the object carries no identity. A subgroup scalar
 * is meaningful only under `HAS_SUBGROUP` and must be zero otherwise; the
 * priority is a byte. Each of those is refused by name, never normalised.
 */
internal object MediaObjectCodec {
    fun statusOf(raw: Long): ObjectStatus =
        when (raw) {
            0L -> ObjectStatus.Normal
            1L -> ObjectStatus.EndOfGroup
            2L -> ObjectStatus.EndOfTrack
            else -> throw UnknownNativeValueException("object status", raw)
        }

    /** RAW or CMAF only; returns true for CMAF. */
    fun packagingOf(raw: Long): Boolean =
        when (raw) {
            1L -> false
            2L -> true
            else -> throw UnknownNativeValueException("object packaging", raw)
        }

    /** The decodable media of a payload: the RAW bytes, or the CMAF mdat slice. */
    fun mediaBytesOf(payload: MediaPayload): MediaBytes =
        when (payload) {
            is MediaPayload.Raw -> payload.payload
            is MediaPayload.Cmaf -> MediaBytes(payload.fragment.bytes().copyOfRange(payload.mdat.offset, payload.mdat.offset + payload.mdat.length))
        }

    /** The object's wire identity from its six carrier scalars, or null when the record did not carry it. */
    fun identityOf(scalars: LongArray): WireIdentity? {
        val group = scalars[ReceiverObjectScalars.GROUP]
        val subgroup = scalars[ReceiverObjectScalars.SUBGROUP]
        val objectId = scalars[ReceiverObjectScalars.OBJECT]
        val priority = scalars[ReceiverObjectScalars.PRIORITY]
        val hasSubgroup = scalars[ReceiverObjectScalars.HAS_SUBGROUP]
        val hasIdentity = scalars[ReceiverObjectScalars.HAS_IDENTITY]
        check(hasIdentity == 0L || hasIdentity == 1L) { "HAS_IDENTITY is a flag; got $hasIdentity" }
        if (hasIdentity == 0L) {
            check(group == 0L && subgroup == 0L && objectId == 0L && priority == 0L && hasSubgroup == 0L) {
                "the carrier claims no identity but carries identity scalars: group $group subgroup $subgroup object $objectId priority $priority hasSubgroup $hasSubgroup"
            }
            return null
        }
        if (priority !in 0L..255L) throw UnknownNativeValueException("publisher priority", priority)
        check(hasSubgroup == 0L || hasSubgroup == 1L) { "HAS_SUBGROUP is a flag; got $hasSubgroup" }
        check(hasSubgroup == 1L || subgroup == 0L) { "the carrier has no subgroup but carries subgroup $subgroup" }
        return WireIdentity(
            group = group.toULong(),
            subgroup = if (hasSubgroup == 1L) subgroup.toULong() else null,
            objectId = objectId.toULong(),
            publisherPriority = priority.toUByte(),
        )
    }

    fun decode(bytes: Array<ByteArray>, scalars: LongArray): BackendObject {
        check(scalars.size >= ReceiverObjectScalars.COUNT) { "object scalars: ${scalars.size} of ${ReceiverObjectScalars.COUNT}" }
        check(bytes.size == ReceiverObjectBytes.COUNT) { "object byte fields: ${bytes.size}, expected ${ReceiverObjectBytes.COUNT}" }
        val cmaf = packagingOf(scalars[ReceiverObjectScalars.PACKAGING])
        val status = statusOf(scalars[ReceiverObjectScalars.STATUS])
        val payload: MediaPayload =
            run {
            if (!cmaf) {
                MediaPayload.Raw(MediaBytes(bytes[ReceiverObjectBytes.PAYLOAD]))
            } else {
                val fragment = bytes[ReceiverObjectBytes.FRAGMENT]
                val offset = scalars[ReceiverObjectScalars.MDAT_OFFSET]
                val length = scalars[ReceiverObjectScalars.MDAT_LEN]
                val count = scalars[ReceiverObjectScalars.SAMPLE_COUNT]
                val packed = bytes[ReceiverObjectBytes.SAMPLES]
                if (status != ObjectStatus.Normal) {
                    // A status object carries no media, and must not claim any.
                    // EVERY media field of the carrier is checked, the payload
                    // included: this branch reads its media from the fragment,
                    // so a payload here would be discarded without a word.
                    val stray = bytes[ReceiverObjectBytes.PAYLOAD]
                    check(fragment.isEmpty() && offset == 0L && length == 0L && count == 0L && packed.isEmpty() && stray.isEmpty()) {
                        "a CMAF object with status $status carries no media; got fragment ${fragment.size}, " +
                            "mdat offset $offset length $length, $count sample(s) in ${packed.size} bytes, " +
                            "payload ${stray.size} bytes"
                    }
                    return@run MediaPayload.Cmaf(MediaBytes(fragment), ByteRange(0, 0), emptyList())
                }
                check(offset >= 0L && length >= 1L) { "a CMAF mdat span is non-negative and at least one byte; got offset $offset length $length" }
                // Both fit an Int before any narrowing, and their sum is taken in 64 bits.
                check(offset <= Int.MAX_VALUE.toLong() && length <= Int.MAX_VALUE.toLong() && offset + length <= fragment.size.toLong()) {
                    "a CMAF mdat span lies inside the fragment; got offset $offset length $length of ${fragment.size}"
                }
                check(count >= 0L && count <= Int.MAX_VALUE.toLong() / ReceiverObjectBytes.SAMPLE_STRIDE) { "sample count $count is not representable" }
                check(packed.size.toLong() == count * ReceiverObjectBytes.SAMPLE_STRIDE) { "the sample carrier is exactly $count x ${ReceiverObjectBytes.SAMPLE_STRIDE} bytes; got ${packed.size}" }
                val buf = ByteBuffer.wrap(packed).order(ByteOrder.LITTLE_ENDIAN)
                val samples = List(count.toInt()) { CmafSample(durationTicks = buf.int.toUInt(), sizeBytes = buf.int.toUInt(), flags = buf.int.toUInt(), compositionOffsetTicks = buf.int) }
                MediaPayload.Cmaf(MediaBytes(fragment), ByteRange(offset.toInt(), length.toInt()), samples)
            }
            }
        return BackendObject(
            track = scalars[ReceiverObjectScalars.TRACK],
            payload = payload,
            status = status,
            endOfGroup = scalars[ReceiverObjectScalars.END_OF_GROUP] != 0L,
            viaDatagram = scalars[ReceiverObjectScalars.DATAGRAM] != 0L,
            keyframe = scalars[ReceiverObjectScalars.KEYFRAME] != 0L,
            captureTime = if (scalars[ReceiverObjectScalars.HAS_CAPTURE] != 0L) UnixMicros(scalars[ReceiverObjectScalars.CAPTURE_US].toULong()) else null,
            decodeTime = MediaMicros(scalars[ReceiverObjectScalars.DECODE_US].toULong()),
            compositionOffset = MediaOffsetMicros(scalars[ReceiverObjectScalars.COMPOSITION_US]),
            presentationTime = MediaMicros(scalars[ReceiverObjectScalars.PRESENTATION_US].toULong()),
            identity = identityOf(scalars),
        )
    }
}
