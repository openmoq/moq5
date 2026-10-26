package org.openmoq.moq5.engine

import org.openmoq.moq5.ByteRange
import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.MediaOffsetMicros
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.ReceiverObjectBytes
import org.openmoq.moq5.jni.ReceiverObjectScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The decoder from the `nativeReceiverPollObject` carrier to a
 * [BackendObject]. `moq_object_status_t` maps only onto the three values
 * `session.h` declares (NORMAL 0, END_OF_GROUP 1, END_OF_TRACK 2); anything
 * else fails closed by field name. Packaging decides the geometry exactly as
 * the C header says: RAW/LOC media is `payload` and `fragment` is empty; CMAF
 * keeps the full `fragment` with an mdat range and the samples, and its
 * `payload` is empty and never read as media. Time domains: capture is
 * `UnixMicros?` (absent when the flag is clear), decode/presentation are
 * `MediaMicros`, the composition offset is signed `MediaOffsetMicros`.
 *
 * Object packaging is EXACTLY RAW=1 or CMAF=2 (`media_object.h`): there is
 * no absent value at the object level, so 0 fails closed like any other
 * unknown value -- this is not the track-description mapper, where 0 means
 * an absent catalog field. For a NORMAL CMAF object the mdat is a [ByteRange]
 * (offset, length): both non-negative, `offset + length <= fragment.size`
 * computed without overflow, and length at least 1 (a NORMAL enqueued CMAF
 * object always parsed a fragment with media; an empty span is refused there,
 * never an ambiguous empty range). The packed sample carrier is exactly
 * `sample_count * 16` bytes, the product computed overflow-safely.
 *
 * A CMAF object whose status is not NORMAL carries no media and is covered by
 * [MediaObjectCodecStatusTest]; every vector here is a NORMAL one.
 */
internal class MediaObjectCodecTest {
    private fun scalars(vararg set: Pair<Int, Long>): LongArray = LongArray(ReceiverObjectScalars.COUNT).also { for ((i, v) in set) it[i] = v }
    private fun bytes(payload: ByteArray = ByteArray(0), fragment: ByteArray = ByteArray(0), samples: ByteArray = ByteArray(0)): Array<ByteArray> =
        Array(ReceiverObjectBytes.COUNT) { ByteArray(0) }.also { it[ReceiverObjectBytes.PAYLOAD] = payload; it[ReceiverObjectBytes.FRAGMENT] = fragment; it[ReceiverObjectBytes.SAMPLES] = samples }
    private fun sample(duration: Int, size: Int, flags: Int, offset: Int): ByteArray =
        java.nio.ByteBuffer.allocate(ReceiverObjectBytes.SAMPLE_STRIDE).order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(duration).putInt(size).putInt(flags).putInt(offset).array()

    @Test
    fun `status maps onto the three declared values and anything else fails closed by name`() {
        assertEquals(ObjectStatus.Normal, MediaObjectCodec.statusOf(0L))
        assertEquals(ObjectStatus.EndOfGroup, MediaObjectCodec.statusOf(1L))
        assertEquals(ObjectStatus.EndOfTrack, MediaObjectCodec.statusOf(2L))
        assertEquals("object status", assertFailsWith<UnknownNativeValueException> { MediaObjectCodec.statusOf(3L) }.field)
        assertEquals("object status", assertFailsWith<UnknownNativeValueException> { MediaObjectCodec.statusOf(-1L) }.field)
    }

    @Test
    fun `a RAW object is the payload, the fragment is empty and no samples exist`() {
        val o = MediaObjectCodec.decode(bytes(payload = "media".toByteArray()), scalars(ReceiverObjectScalars.TRACK to 7L, ReceiverObjectScalars.PACKAGING to 1L, ReceiverObjectScalars.DECODE_US to 10L, ReceiverObjectScalars.PRESENTATION_US to 12L, ReceiverObjectScalars.COMPOSITION_US to -2L))
        assertEquals(MediaPayload.Raw(MediaBytes("media".toByteArray())), o.payload)
        assertEquals(7L, o.track)
        assertEquals(MediaMicros(10uL), o.decodeTime)
        assertEquals(MediaMicros(12uL), o.presentationTime)
        assertEquals(MediaOffsetMicros(-2L), o.compositionOffset)
        assertEquals(null, o.captureTime)
    }

    @Test
    fun `a NORMAL CMAF object keeps the fragment, the mdat span and the samples, and never reads media from its empty payload`() {
        val frag = "moof....mdatMEDIA".toByteArray()
        // Non-palindromic little-endian values and a negative offset: the byte
        // order and the signed field are load-bearing, not coincidental.
        val o = MediaObjectCodec.decode(
            bytes(fragment = frag, samples = sample(0x01020304, 0x0A0B0C05, 0x02000000, -0x00010203) + sample(3000, 0, 0, 0)),
            scalars(ReceiverObjectScalars.PACKAGING to 2L, ReceiverObjectScalars.MDAT_OFFSET to 12L, ReceiverObjectScalars.MDAT_LEN to 5L, ReceiverObjectScalars.SAMPLE_COUNT to 2L),
        )
        val c = o.payload as MediaPayload.Cmaf
        assertEquals(MediaBytes(frag), c.fragment)
        assertEquals(ByteRange(offset = 12, length = 5), c.mdat)
        assertEquals(2, c.samples.size)
        assertEquals(0x01020304u, c.samples[0].durationTicks)
        assertEquals(0x0A0B0C05u, c.samples[0].sizeBytes)
        assertEquals(0x02000000u, c.samples[0].flags)
        assertEquals(-0x00010203, c.samples[0].compositionOffsetTicks)
        assertEquals(3000u, c.samples[1].durationTicks)
        assertEquals("MEDIA", String(MediaObjectCodec.mediaBytesOf(o.payload).bytes()))
    }

    @Test
    fun `object packaging is exactly RAW or CMAF -- zero, three and a negative value fail closed by name`() {
        for (raw in listOf(0L, 3L, -1L, Long.MIN_VALUE)) {
            assertEquals("object packaging", assertFailsWith<UnknownNativeValueException> { MediaObjectCodec.packagingOf(raw) }.field, "value $raw")
            assertFailsWith<UnknownNativeValueException> {
                MediaObjectCodec.decode(bytes(payload = "x".toByteArray()), scalars(ReceiverObjectScalars.PACKAGING to raw))
            }
        }
    }

    @Test
    fun `the mdat span is validated before any object exists -- non-negative, inside the fragment without overflow, at least one byte`() {
        val frag = "moof....mdatMEDIA".toByteArray() // 17 bytes
        fun cmaf(offset: Long, length: Long) = scalars(ReceiverObjectScalars.PACKAGING to 2L, ReceiverObjectScalars.MDAT_OFFSET to offset, ReceiverObjectScalars.MDAT_LEN to length)
        assertEquals(ByteRange(12, 5), (MediaObjectCodec.decode(bytes(fragment = frag), cmaf(12, 5)).payload as MediaPayload.Cmaf).mdat)
        assertEquals(ByteRange(0, 17), (MediaObjectCodec.decode(bytes(fragment = frag), cmaf(0, 17)).payload as MediaPayload.Cmaf).mdat, "the whole fragment is a legal span")
        for ((offset, length) in listOf(-1L to 5L, 12L to -5L, 12L to 6L, 17L to 1L, Int.MAX_VALUE.toLong() to 1L, 1L to Int.MAX_VALUE.toLong(), Long.MAX_VALUE to 1L, 1L to Long.MAX_VALUE, 12L to 0L, 0L to 0L)) {
            assertFailsWith<IllegalStateException>("offset=$offset length=$length") { MediaObjectCodec.decode(bytes(fragment = frag), cmaf(offset, length)) }
        }
    }

    @Test
    fun `the packed sample carrier is exactly sample_count times sixteen bytes, computed without overflow`() {
        val frag = "moof....mdatMEDIA".toByteArray()
        fun cmaf(count: Long) = scalars(ReceiverObjectScalars.PACKAGING to 2L, ReceiverObjectScalars.MDAT_OFFSET to 12L, ReceiverObjectScalars.MDAT_LEN to 5L, ReceiverObjectScalars.SAMPLE_COUNT to count)
        val one = sample(1, 2, 3, -4)
        assertEquals(1, (MediaObjectCodec.decode(bytes(fragment = frag, samples = one), cmaf(1)).payload as MediaPayload.Cmaf).samples.size)
        assertEquals(0, (MediaObjectCodec.decode(bytes(fragment = frag), cmaf(0)).payload as MediaPayload.Cmaf).samples.size, "zero samples, zero bytes")
        assertFailsWith<IllegalStateException>("too short") { MediaObjectCodec.decode(bytes(fragment = frag, samples = one.copyOf(15)), cmaf(1)) }
        assertFailsWith<IllegalStateException>("too long") { MediaObjectCodec.decode(bytes(fragment = frag, samples = one + byteArrayOf(0)), cmaf(1)) }
        assertFailsWith<IllegalStateException>("a count with no bytes") { MediaObjectCodec.decode(bytes(fragment = frag), cmaf(1)) }
        assertFailsWith<IllegalStateException>("bytes with a zero count") { MediaObjectCodec.decode(bytes(fragment = frag, samples = one), cmaf(0)) }
        for (count in listOf(-1L, Long.MAX_VALUE, Long.MAX_VALUE / 16 + 1, (Int.MAX_VALUE.toLong() / 16) + 1)) {
            assertFailsWith<IllegalStateException>("count $count") { MediaObjectCodec.decode(bytes(fragment = frag, samples = one), cmaf(count)) }
        }
    }

    @Test
    fun `a NORMAL CMAF object with an empty fragment or an mdat range outside it is refused, not decoded from payload`() {
        assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(payload = "notmedia".toByteArray()), scalars(ReceiverObjectScalars.PACKAGING to 2L))
        }
        assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(fragment = "short".toByteArray()), scalars(ReceiverObjectScalars.PACKAGING to 2L, ReceiverObjectScalars.MDAT_OFFSET to 4L, ReceiverObjectScalars.MDAT_LEN to 4L))
        }
        assertFailsWith<UnknownNativeValueException> {
            MediaObjectCodec.decode(bytes(payload = "x".toByteArray()), scalars(ReceiverObjectScalars.PACKAGING to 3L))
        }
    }

    @Test
    fun `time domains -- capture follows its flag as UnixMicros, 64-bit values keep all bits, the composition offset keeps its sign`() {
        val o = MediaObjectCodec.decode(bytes(payload = "m".toByteArray()), scalars(ReceiverObjectScalars.PACKAGING to 1L, ReceiverObjectScalars.HAS_CAPTURE to 1L, ReceiverObjectScalars.CAPTURE_US to -1L, ReceiverObjectScalars.COMPOSITION_US to Long.MIN_VALUE, ReceiverObjectScalars.DECODE_US to Long.MIN_VALUE))
        assertEquals(UnixMicros(ULong.MAX_VALUE), o.captureTime)
        assertEquals(MediaOffsetMicros(Long.MIN_VALUE), o.compositionOffset)
        assertEquals(MediaMicros(0x8000_0000_0000_0000uL), o.decodeTime)
        assertTrue(o.keyframe == false && o.endOfGroup == false && o.viaDatagram == false)
    }
}
