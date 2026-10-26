package org.openmoq.moq5.jni

/**
 * The sender carriers: one slot per C field, in each C struct's order. The
 * counts are exact: every JNI row refuses a shorter block before acquiring
 * anything, and every codec refuses a shorter block before reading a slot.
 */

/** The scalar block `nativeSenderAttach` reads (`moq_media_sender_cfg_t`; `block_timeout_us` has no slot). */
internal object SenderAttachScalars {
    const val BACKPRESSURE = 0
    const val QUEUE_MAX_OBJECTS = 1
    const val QUEUE_MAX_BYTES = 2
    const val PRE_READY_MAX_OBJECTS = 3
    const val PRE_READY_MAX_BYTES = 4
    const val VALIDATE_CMAF = 5
    const val PUBLISH_TRACKS = 6
    const val DROP_WITHOUT_DEMAND = 7
    const val CATALOG_REFRESH_INTERVAL_US = 8
    const val COUNT = 9

    /** The out block: the owned child token. */
    const val OUT_COUNT = 1
}

/** The byte fields `nativeSenderAddTrack` reads, a null element meaning absent. */
internal object SendTrackBytes {
    const val NAME = 0
    const val CODEC = 1
    const val INIT_DATA = 2
    const val ROLE = 3
    const val LANG = 4
    const val CHANNEL_CONFIG = 5
    const val COUNT = 6
}

/** The scalar block `nativeSenderAddTrack` reads (`moq_media_track_cfg_t`, struct order; content-protection ref ids are a later slice). */
internal object SendTrackScalars {
    const val MEDIA_TYPE = 0
    const val PACKAGING = 1
    const val TIMESCALE = 2
    const val IS_LIVE = 3
    const val WIDTH = 4
    const val HEIGHT = 5
    const val FRAMERATE_MILLIS = 6
    const val SAMPLERATE = 7
    const val BITRATE = 8
    const val HAS_MAX_GRP_SAP = 9
    const val MAX_GRP_SAP = 10
    const val HAS_MAX_OBJ_SAP = 11
    const val MAX_OBJ_SAP = 12
    const val EMIT_SAP_TIMELINE = 13
    const val SAP_TIMELINE_HISTORY_GROUPS = 14
    const val HAS_TRACK_DURATION = 15
    const val TRACK_DURATION_MS = 16
    const val EMIT_MEDIA_TIMELINE = 17
    const val MEDIA_TIMELINE_HISTORY_GROUPS = 18
    const val HAS_ALT_GROUP = 19
    const val ALT_GROUP = 20
    const val COUNT = 21

    /** The out block: the borrowed track token. */
    const val OUT_COUNT = 1
}

/** The scalar block `nativeSenderWrite` reads (`moq_media_send_object_t`). */
internal object SendObjectScalars {
    const val IS_SYNC = 0
    const val STARTS_GROUP = 1
    const val ENDS_GROUP = 2
    const val DECODE_TIME_US = 3
    const val PRESENTATION_TIME_US = 4
    const val HAS_CAPTURE_TIME = 5
    const val CAPTURE_TIME_US = 6
    const val HAS_SAP_TYPE = 7
    const val SAP_TYPE = 8
    const val COUNT = 9
}

/** The block `nativeSenderProbe` fills: the five state queries. */
internal object SenderProbeScalars {
    const val READY = 0
    const val CLOSED = 1
    const val FATAL = 2
    const val FATAL_CODE = 3
    const val MEDIA_DEMAND = 4
    const val COUNT = 5
}

/** The block `nativeSenderStats` fills: `moq_media_sender_stats_t` in struct order. */
internal object SenderStatsScalars {
    const val OBJECTS_WRITTEN = 0
    const val OBJECTS_SENT = 1
    const val OBJECTS_QUEUED = 2
    const val BYTES_QUEUED = 3
    const val OBJECTS_DROPPED = 4
    const val GROUPS_DROPPED = 5
    const val KEYFRAMES_DROPPED = 6
    const val GROUPS_ABANDONED = 7
    const val BACKPRESSURE_STALLS = 8
    const val LAST_ERROR = 9
    const val SAP_RECORDS_EVICTED = 10
    const val COUNT = 11
}
