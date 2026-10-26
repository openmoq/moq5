package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Domains that must not be conflated, and widths that must not be narrowed.
 *
 * Every expectation here is grounded in the C headers rather than in the older
 * assessment: `moq_result_t` is `int` (types.h), wire error codes are
 * `uint64_t` (session.h), `capture_time_us` is wall-clock microseconds since
 * the Unix epoch and `composition_offset_us` is a SIGNED int64
 * (media_sender.h, media_receiver.h), and `ept_ms` / `media_time_ms` /
 * `wallclock_ms` are uint64 MILLISECONDS (media_receiver.h).
 */
class ValueDomainTest {
    @Test
    fun `a result code and a wire error code are different types`() {
        val r = MoqResultCode(-9)
        val e = MoqErrorCode(9uL)
        assertEquals(-9, r.value)
        assertEquals(9uL, e.value)
        // The domains are distinct types, so no accidental comparison compiles.
        assertNotEquals<Any>(r, e)
    }

    @Test
    fun `a result code is signed, because moq_result_t is int`() {
        assertTrue(MoqResultCode(-1).value < 0)
        assertEquals(Int.MIN_VALUE, MoqResultCode(Int.MIN_VALUE).value)
    }

    /**
     * Values chosen so a 32-bit round trip CANNOT reproduce them.
     *
     * `ULong.MAX_VALUE` is a vacuous width probe on its own: narrowed to `Int`
     * it is `-1`, which widens back to all-ones, so it survives exactly the
     * defect the test exists to catch. Each value here differs from its own
     * narrowed-and-rewidened image, and the pair differs only above bit 32.
     */
    private val widthProbes = listOf(
        1uL shl 32,                       // narrows to 0
        (1uL shl 32) or 1uL,              // narrows to 1
        0xDEADBEEF_00000001uL,            // high half is entirely above bit 32
        ULong.MAX_VALUE - 1uL,
    )

    @Test
    fun `a wire error code carries the full unsigned 64-bit range`() {
        for (v in widthProbes) {
            assertEquals(v, MoqErrorCode(v).value, "error code lost width at $v")
        }
        // differ only above bit 32, so a narrowing carrier would fuse them
        assertNotEquals(MoqErrorCode(1uL shl 32), MoqErrorCode(2uL shl 32))
        assertEquals(ULong.MAX_VALUE, MoqErrorCode(ULong.MAX_VALUE).value)
    }

    @Test
    fun `a request id carries the full unsigned 64-bit range`() {
        for (v in widthProbes) {
            assertEquals(v, RequestId(v).value, "request id lost width at $v")
        }
        assertNotEquals(RequestId(1uL shl 32), RequestId(2uL shl 32))
        assertNotEquals(RequestId(ULong.MAX_VALUE), RequestId(ULong.MAX_VALUE - 1uL))
    }

    @Test
    fun `every unsigned time domain carries the full 64-bit range`() {
        for (v in widthProbes) {
            assertEquals(v, UnixMicros(v).value, "UnixMicros lost width at $v")
            assertEquals(v, MediaMicros(v).value, "MediaMicros lost width at $v")
            assertEquals(v, MediaMillis(v).value, "MediaMillis lost width at $v")
        }
    }

    @Test
    fun `the signed offset domain carries the full 64-bit range`() {
        for (v in listOf(Long.MIN_VALUE, -(1L shl 32), 1L shl 32, Long.MAX_VALUE)) {
            assertEquals(v, MediaOffsetMicros(v).value, "MediaOffsetMicros lost width at $v")
        }
    }

    @Test
    fun `the four time domains are separate types`() {
        val wall = UnixMicros(1_700_000_000_000_000uL)
        val media = MediaMicros(42uL)
        val offset = MediaOffsetMicros(-42L)
        val millis = MediaMillis(7uL)
        assertEquals(1_700_000_000_000_000uL, wall.value)
        assertEquals(42uL, media.value)
        assertEquals(-42L, offset.value)
        assertEquals(7uL, millis.value)
        assertNotEquals<Any>(media, millis)
        assertNotEquals<Any>(media, wall)
    }

    @Test
    fun `a composition offset is signed`() {
        // media_receiver.h: composition_offset_us is int64_t.
        assertTrue(MediaOffsetMicros(-1L).value < 0)
        assertEquals(Long.MIN_VALUE, MediaOffsetMicros(Long.MIN_VALUE).value)
    }

    @Test
    fun `millisecond domains stay milliseconds and are not scaled to micros`() {
        // A value near the top of the uint64 millisecond range would overflow if
        // it were multiplied by 1000 to reach microseconds, so the domain is
        // kept as declared.
        val big = ULong.MAX_VALUE / 2uL
        assertEquals(big, MediaMillis(big).value)
    }

    // The MoqVersion cases live in VersionShapeTest (CorrectionsTest.kt): the
    // shape changed under review 1185 F3, and one contract in one place is the
    // point of moving them rather than editing them here.
}
