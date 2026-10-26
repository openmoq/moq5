package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * MoQ identity is BYTES, not text.
 *
 * `moq_bytes_t` is documented as "not required to be UTF-8 or NUL-terminated"
 * (core/include/moq/types.h), and namespace parts and track names are carried
 * as those bytes. So the Kotlin values must preserve arbitrary bytes exactly,
 * compare by content, and never route identity through `String`.
 */
class ByteIdentityTest {
    private val nonUtf8 = byteArrayOf(0xFF.toByte(), 0xFE.toByte(), 0x00, 0x41)

    @Test
    fun `arbitrary bytes survive a round trip`() {
        val part = NamespacePart(nonUtf8)
        assertContentEquals(nonUtf8, part.bytes())
    }

    @Test
    fun `a NUL byte is content, not a terminator`() {
        val withNul = byteArrayOf(0x61, 0x00, 0x62)
        assertEquals(3, NamespacePart(withNul).size)
        assertNotEquals(NamespacePart(byteArrayOf(0x61)), NamespacePart(withNul))
    }

    @Test
    fun `construction copies, so a later caller mutation cannot change the value`() {
        val caller = byteArrayOf(1, 2, 3)
        val part = NamespacePart(caller)
        caller[0] = 9
        assertContentEquals(byteArrayOf(1, 2, 3), part.bytes())
    }

    @Test
    fun `the accessor copies, so a caller cannot mutate the value through it`() {
        val part = NamespacePart(byteArrayOf(1, 2, 3))
        part.bytes()[0] = 9
        assertContentEquals(byteArrayOf(1, 2, 3), part.bytes())
    }

    @Test
    fun `equality and hashCode are by content, never by array identity`() {
        val a = NamespacePart(byteArrayOf(1, 2, 3))
        val b = NamespacePart(byteArrayOf(1, 2, 3))
        assertEquals(a, b)
        assertEquals(a.hashCode(), b.hashCode())
        assertEquals(setOf(a), setOf(a, b))
    }

    @Test
    fun `two distinct byte strings that are not valid UTF-8 stay distinct`() {
        // Both decode to the replacement character through a lossy String
        // conversion; identity must not collapse them.
        val a = NamespacePart(byteArrayOf(0xFF.toByte()))
        val b = NamespacePart(byteArrayOf(0xFE.toByte()))
        assertNotEquals(a, b)
        assertEquals(a, NamespacePart(byteArrayOf(0xFF.toByte())))
    }

    @Test
    fun `utf8OrNull is an optional view, null for invalid bytes`() {
        assertEquals("ab", NamespacePart(byteArrayOf(0x61, 0x62)).utf8OrNull())
        assertNull(NamespacePart(byteArrayOf(0xFF.toByte())).utf8OrNull())
    }

    @Test
    fun `a namespace part must be non-empty`() {
        // core/include/moq/types.h: "every present part must be non-empty"
        assertFailsWith<IllegalArgumentException> { NamespacePart(ByteArray(0)) }
    }

    @Test
    fun `a track name may be empty and keeps byte identity`() {
        assertEquals(0, TrackName(ByteArray(0)).size)
        assertNotEquals(TrackName(byteArrayOf(0xFF.toByte())), TrackName(byteArrayOf(0xFE.toByte())))
    }

    @Test
    fun `a namespace copies its parts list and compares by content`() {
        val parts = mutableListOf(NamespacePart(byteArrayOf(1)), NamespacePart(byteArrayOf(2)))
        val ns = Namespace(parts)
        parts.add(NamespacePart(byteArrayOf(3)))
        assertEquals(2, ns.parts.size)
        assertEquals(Namespace(listOf(NamespacePart(byteArrayOf(1)), NamespacePart(byteArrayOf(2)))), ns)
    }

    @Test
    fun `a namespace admits zero parts and refuses more than the wire maximum`() {
        // buf.h: a Track Namespace Prefix carries 0..32 fields.
        assertEquals(0, Namespace(emptyList()).parts.size)
        assertTrue(Namespace(List(32) { NamespacePart(byteArrayOf(1)) }).parts.size == 32)
        assertFailsWith<IllegalArgumentException> {
            Namespace(List(33) { NamespacePart(byteArrayOf(1)) })
        }
    }
}
