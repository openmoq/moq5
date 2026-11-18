package org.openmoq.moq5.engine

import org.openmoq.moq5.ByteRange
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.ObjectStatus
import org.openmoq.moq5.jni.ReceiverObjectBytes
import org.openmoq.moq5.jni.ReceiverObjectScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The smallest discriminator for a payload-free CMAF STATUS object.
 *
 * A CMAF track's media rules describe its MEDIA objects. A status object
 * carries none: `moq_media_object_parse` accepts `END_OF_GROUP` with no payload
 * and returns before parsing any media, so the carrier that reaches this
 * decoder is empty in every media field. Requiring an mdat span of at least one
 * byte for those refuses a legal object.
 *
 * What must NOT change is that a NORMAL CMAF object still has to carry real
 * media, and that a status object claiming media is refused rather than
 * normalised into an empty one.
 */
internal class MediaObjectCodecStatusTest {

    private companion object {
        /** CMAF packaging, and nothing else set. */
        fun scalars(
            status: Long,
            mdatOffset: Long = 0L,
            mdatLen: Long = 0L,
            sampleCount: Long = 0L,
        ): LongArray {
            val sc = LongArray(ReceiverObjectScalars.COUNT)
            sc[ReceiverObjectScalars.PACKAGING] = 2L
            sc[ReceiverObjectScalars.STATUS] = status
            sc[ReceiverObjectScalars.MDAT_OFFSET] = mdatOffset
            sc[ReceiverObjectScalars.MDAT_LEN] = mdatLen
            sc[ReceiverObjectScalars.SAMPLE_COUNT] = sampleCount
            return sc
        }

        fun bytes(
            fragment: ByteArray = ByteArray(0),
            samples: ByteArray = ByteArray(0),
            payload: ByteArray = ByteArray(0),
        ): Array<ByteArray> =
            Array(ReceiverObjectBytes.COUNT) {
                when (it) {
                    ReceiverObjectBytes.FRAGMENT -> fragment
                    ReceiverObjectBytes.SAMPLES -> samples
                    ReceiverObjectBytes.PAYLOAD -> payload
                    else -> ByteArray(0)
                }
            }
    }

    @Test
    fun `a payload-free CMAF end-of-group object decodes as CMAF with empty media`() {
        val o = MediaObjectCodec.decode(bytes(), scalars(status = 1L))
        assertEquals(ObjectStatus.EndOfGroup, o.status, "the status must survive")
        val payload = o.payload
        assertTrue(payload is MediaPayload.Cmaf, "a CMAF track's status object stays CMAF; got $payload")
        payload as MediaPayload.Cmaf
        assertEquals(ByteRange(0, 0), payload.mdat, "an empty span")
        assertEquals(0, payload.fragment.bytes().size, "no fragment")
        assertEquals(emptyList(), payload.samples, "no samples")
        assertEquals(0, MediaObjectCodec.mediaBytesOf(payload).bytes().size, "and no decodable media")
    }

    @Test
    fun `a payload-free CMAF end-of-track object decodes the same way`() {
        val o = MediaObjectCodec.decode(bytes(), scalars(status = 2L))
        assertEquals(ObjectStatus.EndOfTrack, o.status)
        assertEquals(ByteRange(0, 0), (o.payload as MediaPayload.Cmaf).mdat)
    }

    @Test
    fun `a NORMAL CMAF object still needs real media`() {
        val e = assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(), scalars(status = 0L))
        }
        assertTrue(
            e.message.orEmpty().contains("at least one byte"),
            "a normal object with no media must still be refused by name: ${e.message}",
        )
    }

    @Test
    fun `a status object that claims a fragment is refused, not normalised`() {
        val e = assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(fragment = byteArrayOf(1, 2, 3)), scalars(status = 1L))
        }
        assertTrue(e.message.orEmpty().contains("carries no media"), "refused by name: ${e.message}")
    }

    @Test
    fun `a status object that claims an mdat span is refused, not normalised`() {
        val e = assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(), scalars(status = 1L, mdatOffset = 0L, mdatLen = 4L))
        }
        assertTrue(e.message.orEmpty().contains("carries no media"), "refused by name: ${e.message}")
        val f = assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(), scalars(status = 1L, mdatOffset = 7L, mdatLen = 0L))
        }
        assertTrue(f.message.orEmpty().contains("carries no media"), "refused by name: ${f.message}")
    }

    @Test
    fun `a status object that claims a payload is refused, not silently discarded`() {
        // The CMAF branch reads its media from the fragment, so a payload here
        // would be dropped without a word. It is part of the same carrier and
        // must be empty like the rest of it.
        val e = assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(payload = byteArrayOf(0x5a)), scalars(status = 1L))
        }
        assertTrue(e.message.orEmpty().contains("carries no media"), "refused by name: ${e.message}")
    }

    @Test
    fun `a status object that claims samples is refused, not normalised`() {
        val e = assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(), scalars(status = 1L, sampleCount = 1L))
        }
        assertTrue(e.message.orEmpty().contains("carries no media"), "refused by name: ${e.message}")
        val f = assertFailsWith<IllegalStateException> {
            MediaObjectCodec.decode(bytes(samples = ByteArray(ReceiverObjectBytes.SAMPLE_STRIDE)), scalars(status = 1L))
        }
        assertTrue(f.message.orEmpty().contains("carries no media"), "refused by name: ${f.message}")
    }
}
