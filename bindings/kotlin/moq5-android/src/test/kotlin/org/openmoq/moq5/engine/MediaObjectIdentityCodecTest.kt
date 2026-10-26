package org.openmoq.moq5.engine

import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaPayload
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.WireIdentity
import org.openmoq.moq5.jni.ReceiverObjectBytes
import org.openmoq.moq5.jni.ReceiverObjectScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

/**
 * The object's wire identity through the `nativeReceiverPollObject` carrier:
 * six scalars after the terminal pair. `HAS_IDENTITY` says the native record
 * covered the complete identity tail; without it the identity is null, which
 * is distinct from an all-zero identity and from an absent subgroup. Ids cross
 * the JNI `jlong` bit-for-bit and are read back as [ULong], so a value above
 * `Long.MAX_VALUE` survives; the priority is the whole 0..255 range. A carrier
 * that claims identity scalars without the flag, a subgroup without the
 * subgroup flag, or a priority outside a byte is refused by name.
 */
internal class MediaObjectIdentityCodecTest {
    private fun scalars(vararg set: Pair<Int, Long>): LongArray = LongArray(ReceiverObjectScalars.COUNT).also { for ((i, v) in set) it[i] = v }
    private fun bytes(payload: ByteArray = "m".toByteArray()): Array<ByteArray> =
        Array(ReceiverObjectBytes.COUNT) { ByteArray(0) }.also { it[ReceiverObjectBytes.PAYLOAD] = payload }
    private fun raw(vararg set: Pair<Int, Long>) =
        MediaObjectCodec.decode(bytes(), scalars(ReceiverObjectScalars.TRACK to 7L, ReceiverObjectScalars.PACKAGING to 1L, *set))

    @Test
    fun `the carrier has six identity scalars after the terminal pair`() {
        assertEquals(17, ReceiverObjectScalars.HAS_IDENTITY)
        assertEquals(18, ReceiverObjectScalars.GROUP)
        assertEquals(19, ReceiverObjectScalars.SUBGROUP)
        assertEquals(20, ReceiverObjectScalars.OBJECT)
        assertEquals(21, ReceiverObjectScalars.PRIORITY)
        assertEquals(22, ReceiverObjectScalars.HAS_SUBGROUP)
        assertEquals(23, ReceiverObjectScalars.COUNT)
    }

    @Test
    fun `high-bit ids cross the jlong bit-for-bit and read back as ULong`() {
        val o = raw(
            ReceiverObjectScalars.HAS_IDENTITY to 1L, ReceiverObjectScalars.GROUP to -1L,
            ReceiverObjectScalars.SUBGROUP to Long.MIN_VALUE, ReceiverObjectScalars.OBJECT to -2L,
            ReceiverObjectScalars.PRIORITY to 255L, ReceiverObjectScalars.HAS_SUBGROUP to 1L,
        )
        assertEquals(WireIdentity(group = ULong.MAX_VALUE, subgroup = 0x8000_0000_0000_0000uL, objectId = ULong.MAX_VALUE - 1uL, publisherPriority = 255u), o.identity)
    }

    @Test
    fun `an all-zero identity is present, not absent`() {
        val o = raw(ReceiverObjectScalars.HAS_IDENTITY to 1L, ReceiverObjectScalars.HAS_SUBGROUP to 1L)
        assertEquals(WireIdentity(0uL, 0uL, 0uL, 0u), o.identity)
    }

    @Test
    fun `a datagram object has an identity without a subgroup`() {
        val o = raw(
            ReceiverObjectScalars.HAS_IDENTITY to 1L, ReceiverObjectScalars.GROUP to 7L, ReceiverObjectScalars.OBJECT to 5L,
            ReceiverObjectScalars.PRIORITY to 200L, ReceiverObjectScalars.DATAGRAM to 1L,
        )
        assertEquals(WireIdentity(group = 7uL, subgroup = null, objectId = 5uL, publisherPriority = 200u), o.identity)
        assertNotEquals(WireIdentity(7uL, 0uL, 5uL, 200u), o.identity, "no subgroup is not subgroup zero")
    }

    @Test
    fun `without the flag the identity is null, and stray identity scalars are refused`() {
        assertNull(raw().identity)
        assertNull(raw(ReceiverObjectScalars.DATAGRAM to 1L).identity)
        val stray = assertFailsWith<IllegalStateException> { raw(ReceiverObjectScalars.GROUP to 1L) }
        assertEquals(true, stray.message!!.contains("identity"))
        assertFailsWith<IllegalStateException> { raw(ReceiverObjectScalars.HAS_SUBGROUP to 1L) }
    }

    @Test
    fun `a subgroup scalar without the subgroup flag, and a priority outside a byte, are refused by name`() {
        assertFailsWith<IllegalStateException> {
            raw(ReceiverObjectScalars.HAS_IDENTITY to 1L, ReceiverObjectScalars.SUBGROUP to 4L)
        }
        for (bad in listOf(256L, -1L)) {
            val e = assertFailsWith<UnknownNativeValueException> {
                raw(ReceiverObjectScalars.HAS_IDENTITY to 1L, ReceiverObjectScalars.HAS_SUBGROUP to 1L, ReceiverObjectScalars.PRIORITY to bad)
            }
            assertEquals("publisher priority", e.field)
        }
    }

    @Test
    fun `HAS_IDENTITY is a flag, so any value other than 0 or 1 is refused before the fields are read`() {
        for (bad in listOf(-1L, 2L)) {
            val e = assertFailsWith<IllegalStateException> {
                raw(ReceiverObjectScalars.HAS_IDENTITY to bad, ReceiverObjectScalars.GROUP to 9L, ReceiverObjectScalars.HAS_SUBGROUP to 1L)
            }
            assertEquals(true, e.message!!.contains("HAS_IDENTITY"), "refused by name: ${e.message}")
            assertFailsWith<IllegalStateException> { raw(ReceiverObjectScalars.HAS_IDENTITY to bad) }
        }
    }

    @Test
    fun `the same numeric identity on two tracks is two objects, and identity does not change the media`() {
        val a = MediaObjectCodec.decode(bytes("a".toByteArray()), scalars(ReceiverObjectScalars.TRACK to 7L, ReceiverObjectScalars.PACKAGING to 1L, ReceiverObjectScalars.HAS_IDENTITY to 1L, ReceiverObjectScalars.GROUP to 9L, ReceiverObjectScalars.OBJECT to 3L, ReceiverObjectScalars.HAS_SUBGROUP to 1L, ReceiverObjectScalars.PRIORITY to 17L))
        val b = MediaObjectCodec.decode(bytes("b".toByteArray()), scalars(ReceiverObjectScalars.TRACK to 9L, ReceiverObjectScalars.PACKAGING to 1L, ReceiverObjectScalars.HAS_IDENTITY to 1L, ReceiverObjectScalars.GROUP to 9L, ReceiverObjectScalars.OBJECT to 3L, ReceiverObjectScalars.HAS_SUBGROUP to 1L, ReceiverObjectScalars.PRIORITY to 17L))
        assertEquals(a.identity, b.identity)
        assertNotEquals(a, b)
        assertEquals(MediaPayload.Raw(MediaBytes("a".toByteArray())), a.payload)
        assertEquals(MediaPayload.Raw(MediaBytes("b".toByteArray())), b.payload)
    }
}
