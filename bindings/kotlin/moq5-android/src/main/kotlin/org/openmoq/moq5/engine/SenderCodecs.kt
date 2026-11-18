package org.openmoq.moq5.engine

import org.openmoq.moq5.MoqResultCode
import org.openmoq.moq5.SapType
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.SendTrackConfig
import org.openmoq.moq5.SenderFatalCode
import org.openmoq.moq5.SenderStats
import org.openmoq.moq5.SenderStatus
import org.openmoq.moq5.SenderTerminal
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.SendObjectScalars
import org.openmoq.moq5.jni.SendTrackBytes
import org.openmoq.moq5.jni.SendTrackScalars
import org.openmoq.moq5.jni.SenderProbeScalars
import org.openmoq.moq5.jni.SenderStatsScalars
import org.openmoq.moq5.nativeOrdinal

/**
 * The encoder to the `nativeSenderWrite` block: booleans as 0/1, the C
 * `has_*` flags as the nullability of the Kotlin field, `decode_time_us`
 * the presentation time when the app declared none, `sap_type` the C
 * `moq_sap_type_t` value (NONE=0, TYPE_1..3=1..3).
 */
internal object SendObjectCodec {
    fun encode(obj: SendObject): LongArray {
        val sc = LongArray(SendObjectScalars.COUNT)
        sc[SendObjectScalars.IS_SYNC] = if (obj.isSync) 1L else 0L
        sc[SendObjectScalars.STARTS_GROUP] = if (obj.startsGroup) 1L else 0L
        sc[SendObjectScalars.ENDS_GROUP] = if (obj.endsGroup) 1L else 0L
        sc[SendObjectScalars.DECODE_TIME_US] = obj.decodeTimeOrPresentation().value.toLong()
        sc[SendObjectScalars.PRESENTATION_TIME_US] = obj.presentationTime.value.toLong()
        sc[SendObjectScalars.HAS_CAPTURE_TIME] = if (obj.captureTime != null) 1L else 0L
        sc[SendObjectScalars.CAPTURE_TIME_US] = obj.captureTime?.value?.toLong() ?: 0L
        sc[SendObjectScalars.HAS_SAP_TYPE] = if (obj.sapType != null) 1L else 0L
        sc[SendObjectScalars.SAP_TYPE] = obj.sapType?.let { sapOrdinal(it) } ?: 0L
        return sc
    }

    private fun sapOrdinal(t: SapType): Long =
        when (t) {
            SapType.None -> 0L
            SapType.Type1 -> 1L
            SapType.Type2 -> 2L
            SapType.Type3 -> 3L
        }
}

/** The encoder to the `nativeSenderAddTrack` byte fields and scalar block (`moq_media_track_cfg_t`). */
internal object SendTrackConfigCodec {
    fun bytes(cfg: SendTrackConfig): Array<ByteArray?> {
        val b = arrayOfNulls<ByteArray>(SendTrackBytes.COUNT)
        b[SendTrackBytes.NAME] = cfg.name.bytes()
        b[SendTrackBytes.CODEC] = cfg.codec.bytes()
        b[SendTrackBytes.INIT_DATA] = cfg.initData?.bytes()
        b[SendTrackBytes.ROLE] = cfg.role?.bytes()
        b[SendTrackBytes.LANG] = cfg.lang?.bytes()
        b[SendTrackBytes.CHANNEL_CONFIG] = cfg.channelConfig?.bytes()
        return b
    }

    fun scalars(cfg: SendTrackConfig): LongArray {
        val sc = LongArray(SendTrackScalars.COUNT)
        sc[SendTrackScalars.MEDIA_TYPE] = cfg.mediaType.nativeOrdinal().toLong()
        sc[SendTrackScalars.PACKAGING] = cfg.packaging.nativeOrdinal().toLong()
        sc[SendTrackScalars.TIMESCALE] = cfg.timescale.toLong()
        sc[SendTrackScalars.IS_LIVE] = if (cfg.isLive) 1L else 0L
        sc[SendTrackScalars.WIDTH] = cfg.width.toLong()
        sc[SendTrackScalars.HEIGHT] = cfg.height.toLong()
        sc[SendTrackScalars.FRAMERATE_MILLIS] = cfg.framerateMillis.toLong()
        sc[SendTrackScalars.SAMPLERATE] = cfg.samplerate.toLong()
        sc[SendTrackScalars.BITRATE] = cfg.bitrate.toLong()
        sc[SendTrackScalars.HAS_MAX_GRP_SAP] = if (cfg.maxGroupSap != null) 1L else 0L
        sc[SendTrackScalars.MAX_GRP_SAP] = cfg.maxGroupSap?.toLong() ?: 0L
        sc[SendTrackScalars.HAS_MAX_OBJ_SAP] = if (cfg.maxObjectSap != null) 1L else 0L
        sc[SendTrackScalars.MAX_OBJ_SAP] = cfg.maxObjectSap?.toLong() ?: 0L
        sc[SendTrackScalars.EMIT_SAP_TIMELINE] = if (cfg.emitSapTimeline) 1L else 0L
        sc[SendTrackScalars.SAP_TIMELINE_HISTORY_GROUPS] = cfg.sapTimelineHistoryGroups.toLong()
        sc[SendTrackScalars.HAS_TRACK_DURATION] = if (cfg.trackDuration != null) 1L else 0L
        sc[SendTrackScalars.TRACK_DURATION_MS] = cfg.trackDuration?.value?.toLong() ?: 0L
        sc[SendTrackScalars.EMIT_MEDIA_TIMELINE] = if (cfg.emitMediaTimeline) 1L else 0L
        sc[SendTrackScalars.MEDIA_TIMELINE_HISTORY_GROUPS] = cfg.mediaTimelineHistoryGroups.toLong()
        sc[SendTrackScalars.HAS_ALT_GROUP] = if (cfg.altGroup != null) 1L else 0L
        sc[SendTrackScalars.ALT_GROUP] = cfg.altGroup?.toLong() ?: 0L
        return sc
    }
}

/**
 * The decoder from the `nativeSenderProbe` block into [SenderStatus]: the
 * four booleans strict (0/1, anything else fails closed by name), and the
 * terminal derived as the C means it -- `is_fatal` covers sender-fatal OR
 * endpoint-fatal and wins; `is_closed` is the endpoint's closed flag and
 * alone means [SenderTerminal.Closed]. A terminal beats readiness: public
 * `ready` is normalised to false whenever a terminal is present, because
 * `write()` refuses in that state and "ready" would be a false "write now".
 */
internal object SenderProbeCodec {
    private fun flag(raw: Long, field: String): Boolean =
        when (raw) {
            0L -> false
            1L -> true
            else -> throw UnknownNativeValueException(field, raw)
        }

    fun decode(sc: LongArray): SenderStatus {
        check(sc.size >= SenderProbeScalars.COUNT) { "probe block: ${sc.size} of ${SenderProbeScalars.COUNT}" }
        val ready = flag(sc[SenderProbeScalars.READY], "ready")
        val closed = flag(sc[SenderProbeScalars.CLOSED], "closed")
        val fatal = flag(sc[SenderProbeScalars.FATAL], "fatal")
        val demand = flag(sc[SenderProbeScalars.MEDIA_DEMAND], "media demand")
        val terminal: SenderTerminal? =
            when {
                fatal -> SenderTerminal.Fatal(SenderFatalCode.fromNative(sc[SenderProbeScalars.FATAL_CODE]))
                closed -> SenderTerminal.Closed
                else -> null
            }
        return SenderStatus(ready = ready && terminal == null, terminal = terminal, mediaDemand = demand)
    }
}

/**
 * The decoder from the `nativeSenderStats` block: one slot per C field in
 * the struct's order, counters reinterpreted with all 64 bits, `last_error`
 * a `moq_result_t` -- an `int` -- so a slot outside the signed 32-bit range
 * fails closed by name.
 */
internal object SenderStatsCodec {
    fun decode(sc: LongArray): SenderStats {
        check(sc.size >= SenderStatsScalars.COUNT) { "stats block: ${sc.size} of ${SenderStatsScalars.COUNT}" }
        val le = sc[SenderStatsScalars.LAST_ERROR]
        if (le < Int.MIN_VALUE.toLong() || le > Int.MAX_VALUE.toLong()) throw UnknownNativeValueException("last error", le)
        return SenderStats(
            objectsWritten = sc[SenderStatsScalars.OBJECTS_WRITTEN].toULong(),
            objectsSent = sc[SenderStatsScalars.OBJECTS_SENT].toULong(),
            objectsQueued = sc[SenderStatsScalars.OBJECTS_QUEUED].toULong(),
            bytesQueued = sc[SenderStatsScalars.BYTES_QUEUED].toULong(),
            objectsDropped = sc[SenderStatsScalars.OBJECTS_DROPPED].toULong(),
            groupsDropped = sc[SenderStatsScalars.GROUPS_DROPPED].toULong(),
            keyframesDropped = sc[SenderStatsScalars.KEYFRAMES_DROPPED].toULong(),
            groupsAbandoned = sc[SenderStatsScalars.GROUPS_ABANDONED].toULong(),
            backpressureStalls = sc[SenderStatsScalars.BACKPRESSURE_STALLS].toULong(),
            lastError = MoqResultCode(le.toInt()),
            sapRecordsEvicted = sc[SenderStatsScalars.SAP_RECORDS_EVICTED].toULong(),
        )
    }
}
