package org.openmoq.moq5.engine

import org.openmoq.moq5.CatalogRevision
import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.ReceiverStatsScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The decoder from the `nativeReceiverStats` scalar block: one slot per C
 * field, in the struct's order; counters reinterpreted as `ULong`; the two
 * booleans strict (0 or 1, anything else fails closed by field name). The
 * block appended after `tracks_malformed` is all or nothing: every field of
 * it is null unless its presence slot is 1, as strict as the booleans; the
 * catalog revision is an open set.
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
            ReceiverStatsScalars.GAPS_REPORTED to 13L, ReceiverStatsScalars.GAP_TAILS_UNKNOWN to 14L,
            ReceiverStatsScalars.REORDER_REFUSED to 15L, ReceiverStatsScalars.TRACKS_MALFORMED to 16L,
        )
        assertEquals(
            ReceiverStats(ULong.MAX_VALUE, 2uL, 0x8000_0000_0000_0000uL, 4uL, 5uL, 6uL, 7uL, 8uL, 9uL, true, 11uL, false, 13uL, 14uL, 15uL, 16uL),
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

    @Test
    fun `the appended block decodes slot by slot with all 64 bits when the stamp covered it`() {
        val sc = block(
            ReceiverStatsScalars.HAS_REVISION_BLOCK to 1L, ReceiverStatsScalars.CATALOG_REVISION to 1L,
            ReceiverStatsScalars.OBJECTS_DECOMPRESSED to -1L, ReceiverStatsScalars.DECOMPRESS_FAILURES to 19L,
            ReceiverStatsScalars.DECOMPRESS_LIMITED to 20L, ReceiverStatsScalars.COMPRESSION_REFUSED to 21L,
            ReceiverStatsScalars.COMPRESSION_CONFLICTS to 22L, ReceiverStatsScalars.COMPRESSION_IGNORED to 23L,
            ReceiverStatsScalars.EVENT_TIMELINE_HEADERS_IGNORED to 24L, ReceiverStatsScalars.LOC_ORDER_GAPS to 25L,
            ReceiverStatsScalars.LOC_ORDER_FORCED to Long.MIN_VALUE,
        )
        val s = ReceiverStatsCodec.decode(sc)
        assertEquals(CatalogRevision.Msf02, s.catalogRevision)
        assertEquals(
            listOf(ULong.MAX_VALUE, 19uL, 20uL, 21uL, 22uL, 23uL, 24uL, 25uL, 0x8000_0000_0000_0000uL),
            listOf(s.objectsDecompressed, s.decompressFailures, s.decompressLimited, s.compressionRefused, s.compressionConflicts, s.compressionIgnored, s.eventTimelineHeadersIgnored, s.locOrderGaps, s.locOrderForced),
        )
        assertEquals(0uL, s.objectsReceived, "the earlier fields are untouched by the block")
    }

    @Test
    fun `without the presence slot every appended field is null, never zero, whatever the slots hold`() {
        val s = ReceiverStatsCodec.decode(block(ReceiverStatsScalars.CATALOG_REVISION to 1L, ReceiverStatsScalars.LOC_ORDER_FORCED to 7L))
        assertEquals(
            listOf<Any?>(null, null, null, null, null, null, null, null, null, null),
            listOf(s.catalogRevision, s.objectsDecompressed, s.decompressFailures, s.decompressLimited, s.compressionRefused, s.compressionConflicts, s.compressionIgnored, s.eventTimelineHeadersIgnored, s.locOrderGaps, s.locOrderForced),
        )
    }

    @Test
    fun `the presence slot is strict, and the revision is an open set`() {
        for (raw in listOf(2L, -1L)) {
            assertEquals("stats revision block", assertFailsWith<UnknownNativeValueException> { ReceiverStatsCodec.decode(block(ReceiverStatsScalars.HAS_REVISION_BLOCK to raw)) }.field, "presence $raw")
        }
        assertEquals(CatalogRevision.Msf01, ReceiverStatsCodec.decode(block(ReceiverStatsScalars.HAS_REVISION_BLOCK to 1L)).catalogRevision, "the native zero is MSF-01")
        assertEquals(CatalogRevision.Unknown(7u), ReceiverStatsCodec.decode(block(ReceiverStatsScalars.HAS_REVISION_BLOCK to 1L, ReceiverStatsScalars.CATALOG_REVISION to 7L)).catalogRevision)
        assertEquals(CatalogRevision.Unknown(UInt.MAX_VALUE), ReceiverStatsCodec.decode(block(ReceiverStatsScalars.HAS_REVISION_BLOCK to 1L, ReceiverStatsScalars.CATALOG_REVISION to 0xFFFF_FFFFL)).catalogRevision)
    }
}
