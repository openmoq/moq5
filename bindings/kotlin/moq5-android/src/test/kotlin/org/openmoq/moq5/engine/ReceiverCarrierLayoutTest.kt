package org.openmoq.moq5.engine

import org.openmoq.moq5.jni.ReceiverEventScalars
import org.openmoq.moq5.jni.ReceiverStatsScalars
import org.openmoq.moq5.jni.TrackDescribeBytes
import org.openmoq.moq5.jni.TrackDescribeScalars
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The receiver carriers' appended slots, at the positions and counts the
 * JNI rows fill and refuse a shorter block against: each block grows only at
 * its end, and every presence bit is a distinct bit of its own mask.
 */
internal class ReceiverCarrierLayoutTest {
    private fun distinctBits(bits: List<Long>) {
        assertEquals(bits.size, bits.toSet().size, "distinct: $bits")
        for (b in bits) assertEquals(1, java.lang.Long.bitCount(b), "one bit each: $b")
    }

    @Test
    fun `the track-event block appends the generation and the two replacement tokens after the fill-loss block`() {
        assertEquals(22, ReceiverEventScalars.FILL_LOSS_OBJECT)
        assertEquals(listOf(23, 24, 25), listOf(ReceiverEventScalars.CONFIG_GENERATION, ReceiverEventScalars.REPLACES, ReceiverEventScalars.REPLACED_BY))
        assertEquals(26, ReceiverEventScalars.COUNT)
        assertEquals(0x100L, ReceiverEventScalars.HAS_REPLACEMENT)
        distinctBits(
            listOf(
                ReceiverEventScalars.HAS_LOCAL_RESULT, ReceiverEventScalars.HAS_INITIATING_ID, ReceiverEventScalars.HAS_FILL_REASON,
                ReceiverEventScalars.HAS_FILL_DOMAIN, ReceiverEventScalars.HAS_FILL_CODE, ReceiverEventScalars.HAS_DELIVERY_ORIGIN,
                ReceiverEventScalars.HAS_DELIVERY_ID, ReceiverEventScalars.HAS_FILL_LOSS, ReceiverEventScalars.HAS_REPLACEMENT,
            ),
        )
    }

    @Test
    fun `the describe block appends the generation block and its presence mask after is_live`() {
        assertEquals(34, TrackDescribeScalars.IS_LIVE)
        assertEquals(
            listOf(35, 36, 37, 38),
            listOf(TrackDescribeScalars.DESC_GENERATION, TrackDescribeScalars.INIT_SOURCE, TrackDescribeScalars.INIT_PROPERTY_TYPE, TrackDescribeScalars.HAS),
        )
        assertEquals(39, TrackDescribeScalars.COUNT)
        distinctBits(
            listOf(
                TrackDescribeScalars.HAS_GENERATION, TrackDescribeScalars.HAS_ENCRYPTION_SCHEME, TrackDescribeScalars.HAS_CIPHER_SUITE,
                TrackDescribeScalars.HAS_KEY_ID, TrackDescribeScalars.HAS_TRACK_BASE_KEY,
            ),
        )
    }

    @Test
    fun `the describe byte fields append the four encryption fields before depends and the ref ids`() {
        assertEquals(12, TrackDescribeBytes.CENC_DEFAULT_KID)
        assertEquals(
            listOf(13, 14, 15, 16),
            listOf(TrackDescribeBytes.ENCRYPTION_SCHEME, TrackDescribeBytes.CIPHER_SUITE, TrackDescribeBytes.KEY_ID, TrackDescribeBytes.TRACK_BASE_KEY),
        )
        assertEquals(17, TrackDescribeBytes.FIXED)
    }

    @Test
    fun `the stats block appends its presence slot and the ten fields after tracks_malformed, in the struct's order`() {
        assertEquals(15, ReceiverStatsScalars.TRACKS_MALFORMED)
        assertEquals(
            (16..26).toList(),
            listOf(
                ReceiverStatsScalars.HAS_REVISION_BLOCK, ReceiverStatsScalars.CATALOG_REVISION, ReceiverStatsScalars.OBJECTS_DECOMPRESSED,
                ReceiverStatsScalars.DECOMPRESS_FAILURES, ReceiverStatsScalars.DECOMPRESS_LIMITED, ReceiverStatsScalars.COMPRESSION_REFUSED,
                ReceiverStatsScalars.COMPRESSION_CONFLICTS, ReceiverStatsScalars.COMPRESSION_IGNORED,
                ReceiverStatsScalars.EVENT_TIMELINE_HEADERS_IGNORED, ReceiverStatsScalars.LOC_ORDER_GAPS, ReceiverStatsScalars.LOC_ORDER_FORCED,
            ),
        )
        assertEquals(27, ReceiverStatsScalars.COUNT)
    }
}
