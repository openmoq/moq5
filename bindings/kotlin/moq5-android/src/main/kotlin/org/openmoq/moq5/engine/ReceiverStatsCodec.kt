package org.openmoq.moq5.engine

import org.openmoq.moq5.CatalogRevision
import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.ReceiverStatsScalars

/**
 * The decoder from the `nativeReceiverStats` block: one slot per C field,
 * counters reinterpreted as `ULong` with all their bits, the two booleans
 * strict -- 0 or 1, anything else fails closed by field name. The appended
 * block is all or nothing: every field of it is null unless the block's
 * presence slot (as strict as the booleans) says the library reported it.
 */
internal object ReceiverStatsCodec {
    private fun flag(raw: Long, field: String): Boolean =
        when (raw) {
            0L -> false
            1L -> true
            else -> throw UnknownNativeValueException(field, raw)
        }

    fun decode(sc: LongArray): ReceiverStats {
        check(sc.size >= ReceiverStatsScalars.COUNT) { "stats block: ${sc.size} of ${ReceiverStatsScalars.COUNT}" }
        val block = flag(sc[ReceiverStatsScalars.HAS_REVISION_BLOCK], "stats revision block")
        fun counter(i: Int): ULong? = if (block) sc[i].toULong() else null
        return ReceiverStats(
            objectsReceived = sc[ReceiverStatsScalars.OBJECTS_RECEIVED].toULong(),
            objectsQueued = sc[ReceiverStatsScalars.OBJECTS_QUEUED].toULong(),
            bytesQueued = sc[ReceiverStatsScalars.BYTES_QUEUED].toULong(),
            objectsDropped = sc[ReceiverStatsScalars.OBJECTS_DROPPED].toULong(),
            groupsDropped = sc[ReceiverStatsScalars.GROUPS_DROPPED].toULong(),
            keyframesDropped = sc[ReceiverStatsScalars.KEYFRAMES_DROPPED].toULong(),
            parseDrops = sc[ReceiverStatsScalars.PARSE_DROPS].toULong(),
            overflowEvents = sc[ReceiverStatsScalars.OVERFLOW_EVENTS].toULong(),
            pauseTransitions = sc[ReceiverStatsScalars.PAUSE_TRANSITIONS].toULong(),
            paused = flag(sc[ReceiverStatsScalars.PAUSED], "paused"),
            catalogDrops = sc[ReceiverStatsScalars.CATALOG_DROPS].toULong(),
            catalogComplete = flag(sc[ReceiverStatsScalars.CATALOG_COMPLETE], "catalog complete"),
            gapsReported = sc[ReceiverStatsScalars.GAPS_REPORTED].toULong(),
            gapTailsUnknown = sc[ReceiverStatsScalars.GAP_TAILS_UNKNOWN].toULong(),
            reorderRefused = sc[ReceiverStatsScalars.REORDER_REFUSED].toULong(),
            tracksMalformed = sc[ReceiverStatsScalars.TRACKS_MALFORMED].toULong(),
            catalogRevision = if (block) CatalogRevision.ofNative(sc[ReceiverStatsScalars.CATALOG_REVISION].toUInt()) else null,
            objectsDecompressed = counter(ReceiverStatsScalars.OBJECTS_DECOMPRESSED),
            decompressFailures = counter(ReceiverStatsScalars.DECOMPRESS_FAILURES),
            decompressLimited = counter(ReceiverStatsScalars.DECOMPRESS_LIMITED),
            compressionRefused = counter(ReceiverStatsScalars.COMPRESSION_REFUSED),
            compressionConflicts = counter(ReceiverStatsScalars.COMPRESSION_CONFLICTS),
            compressionIgnored = counter(ReceiverStatsScalars.COMPRESSION_IGNORED),
            eventTimelineHeadersIgnored = counter(ReceiverStatsScalars.EVENT_TIMELINE_HEADERS_IGNORED),
            locOrderGaps = counter(ReceiverStatsScalars.LOC_ORDER_GAPS),
            locOrderForced = counter(ReceiverStatsScalars.LOC_ORDER_FORCED),
        )
    }
}
