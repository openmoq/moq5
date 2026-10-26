package org.openmoq.moq5.engine

import org.openmoq.moq5.ReceiverStats
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.ReceiverStatsScalars

/**
 * The decoder from the `nativeReceiverStats` block: one slot per C field,
 * counters reinterpreted as `ULong` with all their bits, the two booleans
 * strict -- 0 or 1, anything else fails closed by field name.
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
            sapRecordDeferrals = sc[ReceiverStatsScalars.SAP_RECORD_DEFERRALS].toULong(),
            mediaTimelineRecordDeferrals = sc[ReceiverStatsScalars.MEDIA_TIMELINE_RECORD_DEFERRALS].toULong(),
        )
    }
}
