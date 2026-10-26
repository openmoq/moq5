package org.openmoq.moq5.engine

import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.SenderStats
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.SenderStatsScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The decoder from the `nativeSenderStats` block: one slot per
 * `moq_media_sender_stats_t` field in the struct's order, counters
 * reinterpreted with all 64 bits, `last_error` a `moq_result_t` -- an
 * `int` -- so a slot outside the signed 32-bit range fails closed by name.
 */
internal class SenderStatsCodecTest {
    private fun block(vararg set: Pair<Int, Long>) = LongArray(SenderStatsScalars.COUNT).also { for ((i, v) in set) it[i] = v }

    @Test
    fun `each slot maps to its own field with all 64 bits`() {
        val sc = block(
            SenderStatsScalars.OBJECTS_WRITTEN to -1L, SenderStatsScalars.OBJECTS_SENT to 2L, SenderStatsScalars.OBJECTS_QUEUED to Long.MIN_VALUE,
            SenderStatsScalars.BYTES_QUEUED to 4L, SenderStatsScalars.OBJECTS_DROPPED to 5L, SenderStatsScalars.GROUPS_DROPPED to 6L,
            SenderStatsScalars.KEYFRAMES_DROPPED to 7L, SenderStatsScalars.GROUPS_ABANDONED to 8L, SenderStatsScalars.BACKPRESSURE_STALLS to 9L,
            SenderStatsScalars.LAST_ERROR to -10L, SenderStatsScalars.SAP_RECORDS_EVICTED to 11L,
        )
        assertEquals(
            SenderStats(ULong.MAX_VALUE, 2uL, 0x8000_0000_0000_0000uL, 4uL, 5uL, 6uL, 7uL, 8uL, 9uL, MoqResultCode(-10), 11uL),
            SenderStatsCodec.decode(sc),
        )
        assertEquals(11, SenderStatsScalars.COUNT)
    }

    @Test
    fun `last_error is a signed 32-bit result -- a slot outside that range fails closed by field name`() {
        assertEquals(MoqResultCode(Int.MIN_VALUE), SenderStatsCodec.decode(block(SenderStatsScalars.LAST_ERROR to Int.MIN_VALUE.toLong())).lastError)
        assertEquals("last error", assertFailsWith<UnknownNativeValueException> { SenderStatsCodec.decode(block(SenderStatsScalars.LAST_ERROR to Int.MAX_VALUE.toLong() + 1L)) }.field)
        assertEquals("last error", assertFailsWith<UnknownNativeValueException> { SenderStatsCodec.decode(block(SenderStatsScalars.LAST_ERROR to Int.MIN_VALUE.toLong() - 1L)) }.field)
    }

    @Test
    fun `a short block is refused before any field is read`() {
        assertFailsWith<IllegalStateException> { SenderStatsCodec.decode(LongArray(SenderStatsScalars.COUNT - 1)) }
    }
}
