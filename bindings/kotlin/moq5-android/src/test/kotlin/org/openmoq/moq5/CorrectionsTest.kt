package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNotSame
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** F1: a returned collection must not be a handle on the value's own storage. */
class NamespaceImmutabilityTest {
    private fun ns() =
        Namespace(listOf(NamespacePart(byteArrayOf(1)), NamespacePart(byteArrayOf(2))))

    @Test
    fun `casting the returned list to a mutable type cannot change the namespace`() {
        val n = ns()
        val before = n.parts
        val hashBefore = n.hashCode()
        (n.parts as MutableList<*>).removeAt(0)
        assertEquals(2, n.partCount)
        assertEquals(before, n.parts)
        assertEquals(hashBefore, n.hashCode())
        assertEquals(ns(), n)
    }

    @Test
    fun `a namespace already in a set keeps its identity after the cast-and-mutate`() {
        val n = ns()
        val set = hashSetOf(n)
        (n.parts as MutableList<*>).clear()
        assertTrue(n in set)
        assertTrue(ns() in set)
    }

    @Test
    fun `each accessor call yields an independent snapshot`() {
        val n = ns()
        // Both snapshots are held, and reference inequality is asserted
        // directly: identity hash codes can collide, and an unheld temporary
        // can be collected and its identity reused.
        val first = n.parts
        val second = n.parts
        assertNotSame(first, second)
        assertEquals(first, second)
    }
}

/** F2: the 4096-byte Track Namespace / Full Track Name bound. */
class FullTrackNameBoundTest {
    private fun part(n: Int) = NamespacePart(ByteArray(n) { 0x61 })

    @Test
    fun `a namespace of exactly 4096 value bytes is accepted`() {
        assertEquals(4096, Namespace(listOf(part(4096))).valueByteCount)
    }

    @Test
    fun `a namespace of 4097 value bytes is rejected`() {
        assertFailsWith<IllegalArgumentException> { Namespace(listOf(part(4096), part(1))) }
    }

    @Test
    fun `the bound is on the TOTAL, spread across parts`() {
        assertEquals(4096, Namespace(List(32) { part(128) }).valueByteCount)
        assertFailsWith<IllegalArgumentException> { Namespace(List(32) { part(129) }) }
    }

    @Test
    fun `a track name alone cannot exceed 4096`() {
        assertEquals(4096, TrackName(ByteArray(4096)).size)
        assertFailsWith<IllegalArgumentException> { TrackName(ByteArray(4097)) }
    }

    @Test
    fun `a full track name is bounded by the COMBINED size`() {
        // Each component is individually legal; together they are not. A gate
        // that checked the two separately would admit this.
        val ns = Namespace(listOf(part(4000)))
        assertEquals(4096, FullTrackName(ns, TrackName(ByteArray(96))).byteCount)
        assertFailsWith<IllegalArgumentException> {
            FullTrackName(ns, TrackName(ByteArray(97)))
        }
    }

    @Test
    fun `an empty track name and a zero-part namespace are both legal`() {
        val f = FullTrackName(Namespace(emptyList()), TrackName(ByteArray(0)))
        assertEquals(0, f.byteCount)
        assertEquals(0, f.namespace.partCount)
    }

    @Test
    fun `a zero-part namespace still admits a full-size track name`() {
        assertEquals(
            4096,
            FullTrackName(Namespace(emptyList()), TrackName(ByteArray(4096))).byteCount,
        )
    }

    @Test
    fun `the profile-neutral namespace does not impose draft-16's nonzero rule`() {
        // A Track Namespace Prefix legitimately has zero fields ("match all");
        // the at-least-one-field rule is a negotiated-profile concern.
        assertEquals(0, Namespace(emptyList()).partCount)
    }
}

/** F3: the version is a nonzero UInt value, with no Unknown wrapper. */
class VersionShapeTest {
    @Test
    fun `known drafts have exactly one representation`() {
        assertEquals(MoqVersion.Draft16, MoqVersion(16u))
        assertEquals(MoqVersion.Draft18, MoqVersion(18u))
        assertEquals(MoqVersion.Draft16.hashCode(), MoqVersion(16u).hashCode())
        assertEquals(16u, MoqVersion.Draft16.code)
    }

    @Test
    fun `an unknown nonzero version round-trips as itself`() {
        assertEquals(19u, MoqVersion(19u).code)
        assertEquals(MoqVersion(19u), MoqVersion(19u))
        assertNotEquals(MoqVersion.Draft18, MoqVersion(19u))
    }

    @Test
    fun `the maximum UInt is carried`() {
        assertEquals(UInt.MAX_VALUE, MoqVersion(UInt.MAX_VALUE).code)
        assertNotEquals(MoqVersion(UInt.MAX_VALUE), MoqVersion(UInt.MAX_VALUE - 1u))
    }

    @Test
    fun `zero is rejected and is never draft-16`() {
        assertFailsWith<IllegalArgumentException> { MoqVersion(0u) }
        assertNull(MoqVersion.fromNativeOrNull(0u))
        assertNotEquals<Any?>(MoqVersion.Draft16, MoqVersion.fromNativeOrNull(0u))
    }

    @Test
    fun `a native nonzero value maps to that version, named or not`() {
        assertEquals(MoqVersion.Draft18, MoqVersion.fromNativeOrNull(18u))
        assertEquals(MoqVersion(4242u), MoqVersion.fromNativeOrNull(4242u))
    }
}

/** F4: wallclock milliseconds are not media milliseconds. */
class MillisDomainTest {
    @Test
    fun `the two millisecond domains are distinct types`() {
        val media = MediaMillis(7uL)
        val wall = UnixMillis(7uL)
        assertNotEquals<Any>(media, wall)
        assertEquals(7uL, media.value)
        assertEquals(7uL, wall.value)
    }

    @Test
    fun `both millisecond domains carry the full unsigned 64-bit range`() {
        for (v in listOf(1uL shl 32, (1uL shl 32) or 1uL, ULong.MAX_VALUE - 1uL)) {
            assertEquals(v, MediaMillis(v).value, "MediaMillis lost width at $v")
            assertEquals(v, UnixMillis(v).value, "UnixMillis lost width at $v")
        }
    }

    @Test
    fun `a native wallclock zero is ABSENT, not the epoch instant`() {
        // media/msf/include/moq/msf.h records 0 for VOD: no live wallclock.
        assertNull(UnixMillis.fromNativeOrNull(0uL))
        assertEquals(UnixMillis(1uL), UnixMillis.fromNativeOrNull(1uL))
    }

    @Test
    fun `an explicit epoch instant is still constructible`() {
        // The mapping rule lives at the boundary; the value itself is not
        // forbidden from naming the epoch.
        assertEquals(0uL, UnixMillis(0uL).value)
    }
}

/** F6: diagnostics must be ASCII-safe and unambiguous for arbitrary bytes. */
class DiagnosticRenderingTest {
    @Test
    fun `valid ASCII is rendered as hex, not echoed`() {
        assertEquals("NamespacePart(hex:6162)", NamespacePart(byteArrayOf(0x61, 0x62)).toString())
    }

    @Test
    fun `control bytes cannot reach a log line literally`() {
        // newline, carriage return, tab, ESC, 'A'
        val s = NamespacePart(byteArrayOf(0x0A, 0x0D, 0x09, 0x1B, 0x41)).toString()
        for (c in s) {
            assertTrue(c.code in 0x20..0x7E, "non-printable char U+${c.code.toString(16)} in $s")
        }
        assertEquals("NamespacePart(hex:0a0d091b41)", s)
    }

    @Test
    fun `a NUL byte is rendered, not truncated`() {
        assertEquals("TrackName(hex:610062)", TrackName(byteArrayOf(0x61, 0x00, 0x62)).toString())
    }

    @Test
    fun `invalid UTF-8 renders deterministically`() {
        assertEquals(
            "NamespacePart(hex:fffe)",
            NamespacePart(byteArrayOf(0xFF.toByte(), 0xFE.toByte())).toString(),
        )
    }

    @Test
    fun `leading zero bytes stay visible`() {
        assertEquals("TrackName(hex:0001)", TrackName(byteArrayOf(0x00, 0x01)).toString())
    }

    @Test
    fun `two distinct identities render differently`() {
        assertNotEquals(
            NamespacePart(byteArrayOf(0x01)).toString(),
            NamespacePart(byteArrayOf(0x02)).toString(),
        )
    }

    @Test
    fun `rendering does not depend on the default locale`() {
        val saved = java.util.Locale.getDefault()
        try {
            // A locale whose digits and casing rules differ from the root's.
            java.util.Locale.setDefault(java.util.Locale.forLanguageTag("ar-EG-u-nu-arab"))
            assertEquals(
                "TrackName(hex:0a1bff)",
                TrackName(byteArrayOf(0x0A, 0x1B, 0xFF.toByte())).toString(),
            )
        } finally {
            java.util.Locale.setDefault(saved)
        }
    }
}
