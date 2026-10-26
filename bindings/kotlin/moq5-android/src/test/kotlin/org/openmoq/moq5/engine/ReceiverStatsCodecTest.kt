package org.openmoq.moq5.engine

import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.ReceiverStatsScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The decoder from the `nativeReceiverStats` scalar block: one slot per C
 * field, in the struct's order; counters reinterpreted as `ULong`; the two
 * booleans strict (0 or 1, anything else fails closed by field name).
 */
internal class ReceiverStatsCodecTest {
    private fun block(vararg set: Pair<Int, Long>) = LongArray(ReceiverStatsScalars.COUNT).also { for ((i, v) in set) it[i] = v }

    @Test
    fun `each slot maps to its own field with all 64 bits`() {
        val sc = block(
            ReceiverStatsScalars.OBJECTS_RECEIVED to -1L, ReceiverStatsScalars.OBJECTS_QUEUED to 2L, ReceiverStatsScalars.BYTES_QUEUED to Long.MIN_VALUE,
            ReceiverStatsScalars.OBJECTS_DROPPED to 4L, ReceiverStatsScalars.GROUPS_DROPPED to 5L, ReceiverStatsScalars.KEYFRAMES_DROPPED to 6L,
            ReceiverStatsScalars.PARSE_DROPS to 7L, ReceiverStatsScalars.OVERFLOW_EVENTS to 8L, ReceiverStatsScalars.PAUSE_TRANSITIONS to 9L,
            ReceiverStatsScalars.PAUSED to 1L, ReceiverStatsScalars.CATALOG_DROPS to 11L, ReceiverStatsScalars.CATALOG_COMPLETE to 0L,
            ReceiverStatsScalars.SAP_RECORD_DEFERRALS to 13L, ReceiverStatsScalars.MEDIA_TIMELINE_RECORD_DEFERRALS to 14L,
        )
        assertEquals(
            ReceiverStats(ULong.MAX_VALUE, 2uL, 0x8000_0000_0000_0000uL, 4uL, 5uL, 6uL, 7uL, 8uL, 9uL, true, 11uL, false, 13uL, 14uL),
            ReceiverStatsCodec.decode(sc),
        )
    }

    @Test
    fun `the booleans are strict -- only 0 and 1 decode, anything else fails closed by field name`() {
        assertEquals(false, ReceiverStatsCodec.decode(block(ReceiverStatsScalars.PAUSED to 0L)).paused)
        assertEquals(true, ReceiverStatsCodec.decode(block(ReceiverStatsScalars.CATALOG_COMPLETE to 1L)).catalogComplete)
        for (raw in listOf(2L, -1L, Long.MIN_VALUE)) {
            assertEquals("paused", assertFailsWith<UnknownNativeValueException> { ReceiverStatsCodec.decode(block(ReceiverStatsScalars.PAUSED to raw)) }.field, "paused $raw")
            assertEquals("catalog complete", assertFailsWith<UnknownNativeValueException> { ReceiverStatsCodec.decode(block(ReceiverStatsScalars.CATALOG_COMPLETE to raw)) }.field, "catalog complete $raw")
        }
    }

    @Test
    fun `a short block is refused before any field is read`() {
        assertFailsWith<IllegalStateException> { ReceiverStatsCodec.decode(LongArray(ReceiverStatsScalars.COUNT - 1)) }
    }
}
