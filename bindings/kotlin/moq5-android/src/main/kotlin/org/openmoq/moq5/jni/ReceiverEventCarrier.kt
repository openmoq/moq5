package org.openmoq.moq5.jni

/*
 * The carriers of the receiver EVENT rows: fixed scalar blocks (a `long[]`
 * the native side fills) and, for a description, the byte fields as a
 * `byte[][]` in a fixed order. These tables are the one place the slot
 * order is written down; the C rows fill them by the same names.
 */

/** The scalar block `nativeReceiverPollTrack` fills: one slot per field, in this order. */
internal object ReceiverEventScalars {
    const val KIND = 0            // moq_media_track_event_kind_t (0 when the answer is not an event)
    const val TRACK = 1           // the BORROWED child token of the track; 0 for CATALOG_READY
    const val HAS_LARGEST = 2
    const val LARGEST_GROUP = 3
    const val LARGEST_OBJECT = 4
    const val HAS_EXPIRES = 5
    const val EXPIRES_MS = 6
    const val PARSE_DROP_CLASS = 7
    const val PARSE_DROPS_TOTAL = 8
    const val PARSE_DROPS_DELTA = 9
    const val TERMINAL_FATAL = 10 // on MOQ_ERR_CLOSED: 1 when the receiver is fatal
    const val TERMINAL_CODE = 11  // on MOQ_ERR_CLOSED: the receiver's fatal code, full width
    const val COUNT = 12
}

/**
 * The scalar block `nativeTrackDescribe` fills. Catalog passthrough
 * dimensions (`has_width`/`width`, ...) and CMAF init-derived dimensions
 * (`init.width`, ...) are DISTINCT slots. `has_template`/`template_` does
 * not cross in this slice (deferred with SAP/timeline).
 */
internal object TrackDescribeScalars {
    const val RESULT = 0            // MOQ_OK, or the refusal (the byte array is then null)
    const val MEDIA_TYPE = 1        // moq_media_type_t; 0 absent
    const val PACKAGING_KIND = 2    // moq_media_packaging_t; 0 absent
    const val TIMESCALE = 3         // info.timescale
    const val TRANSPORT_VERSION = 4
    const val HAS_INIT = 5
    const val INIT_CODEC_KIND = 6   // moq_cmaf_codec_kind_t; 0 UNKNOWN -> absent
    const val INIT_TIMESCALE = 7
    const val INIT_WIDTH = 8
    const val INIT_HEIGHT = 9
    const val INIT_SAMPLERATE = 10
    const val INIT_CHANNEL_COUNT = 11
    const val INIT_TRACK_ID = 12
    const val HAS_CENC = 13
    const val CENC_DEFAULT_IS_PROTECTED = 14
    const val CENC_IV_SIZE = 15
    const val HAS_WIDTH = 16
    const val CATALOG_WIDTH = 17
    const val HAS_HEIGHT = 18
    const val CATALOG_HEIGHT = 19
    const val HAS_SAMPLERATE = 20
    const val CATALOG_SAMPLERATE = 21
    const val HAS_FRAMERATE = 22
    const val FRAMERATE_MILLIS = 23
    const val HAS_BITRATE = 24
    const val BITRATE = 25
    const val HAS_MAX_GRP_SAP = 26
    const val MAX_GRP_SAP = 27
    const val HAS_MAX_OBJ_SAP = 28
    const val MAX_OBJ_SAP = 29
    const val DEPENDS_COUNT = 30
    const val CP_REF_ID_COUNT = 31
    const val HAS_TRACK_DURATION = 32
    const val TRACK_DURATION_MS = 33
    const val IS_LIVE = 34
    const val COUNT = 35
}

/** The byte-field order of the array `nativeTrackDescribe` returns: the fixed fields, then `depends`, then the content-protection ref ids. */
internal object TrackDescribeBytes {
    const val NAME = 0
    const val ROLE = 1
    const val CODEC = 2
    const val LANG = 3
    const val LABEL = 4
    const val PACKAGING = 5
    const val MIME_TYPE = 6
    const val EVENT_TYPE = 7
    const val CHANNEL_CONFIG = 8
    const val INIT_DATA = 9
    const val CODEC_CONFIG = 10
    const val CENC_SCHEME = 11
    const val CENC_DEFAULT_KID = 12
    const val FIXED = 13
}

/**
 * The scalar block `nativeReceiverPollObject` fills. On any answer but an
 * object, [RESULT] carries the code and the array is null: `MOQ_DONE`,
 * `MOQ_ERR_INTERRUPTED`, `MOQ_ERR_CLOSED` (with the terminal slots), or a
 * registry code (a refused token mint included).
 */
internal object ReceiverObjectScalars {
    const val RESULT = 0
    const val TRACK = 1             // the track's BORROWED token from the first-sight map (minted here if this is the handle's first sight)
    const val PACKAGING = 2         // moq_media_packaging_t: RAW=1, CMAF=2, nothing else
    const val STATUS = 3            // moq_object_status_t: 0, 1, 2
    const val END_OF_GROUP = 4
    const val DATAGRAM = 5
    const val KEYFRAME = 6
    const val HAS_CAPTURE = 7
    const val CAPTURE_US = 8        // wall clock, microseconds since the Unix epoch
    const val DECODE_US = 9
    const val COMPOSITION_US = 10   // signed
    const val PRESENTATION_US = 11
    const val MDAT_OFFSET = 12      // CMAF: the media span inside FRAGMENT
    const val MDAT_LEN = 13
    const val SAMPLE_COUNT = 14
    const val TERMINAL_FATAL = 15   // on MOQ_ERR_CLOSED
    const val TERMINAL_CODE = 16    // on MOQ_ERR_CLOSED: the receiver's own code, full width
    // The wire identity, present only when the native stamp covered the complete identity tail
    // (HAS_IDENTITY); without it the six are zero and the object carries no identity. The ids are
    // the C uint64 values bit-for-bit in the jlong; HAS_SUBGROUP clear means the wire carried no
    // subgroup (a datagram), and SUBGROUP is then zero.
    const val HAS_IDENTITY = 17
    const val GROUP = 18
    const val SUBGROUP = 19
    const val OBJECT = 20
    const val PRIORITY = 21         // the session-delivered publisher priority, 0..255
    const val HAS_SUBGROUP = 22
    const val COUNT = 23
}

/** The byte arrays `nativeReceiverPollObject` returns: the RAW payload, the CMAF fragment, and the samples packed at [SAMPLE_STRIDE]. */
internal object ReceiverObjectBytes {
    const val PAYLOAD = 0
    const val FRAGMENT = 1
    const val SAMPLES = 2           // per sample, little-endian: duration u32, size u32, flags u32, composition_offset i32
    const val COUNT = 3
    const val SAMPLE_STRIDE = 16
}

/**
 * The scalar block `nativeReceiverPollSap` fills: the record on `MOQ_OK`;
 * [DEFERRALS] on every answer (read after the poll, under the same registry
 * pin); the terminal slots on `MOQ_ERR_CLOSED`.
 */
internal object ReceiverSapScalars {
    const val TRACK = 0             // the track's BORROWED token from the first-sight map
    const val GROUP = 1             // rec.group
    const val OBJECT = 2            // rec.object
    const val SAP_TYPE = 3          // moq_sap_type_t
    const val EPT_MS = 4            // rec.ept_ms, milliseconds
    const val DEFERRALS = 5         // stats.sap_record_deferrals
    const val TERMINAL_FATAL = 6
    const val TERMINAL_CODE = 7
    const val COUNT = 8
}

/** The scalar block `nativeReceiverPollMediaTimeline` fills, the same shape as [ReceiverSapScalars]. */
internal object ReceiverTimelineScalars {
    const val TRACK = 0
    const val MEDIA_TIME_MS = 1     // rec.media_time_ms
    const val GROUP = 2
    const val OBJECT = 3
    const val WALLCLOCK_MS = 4      // rec.wallclock_ms; 0 = absent
    const val DEFERRALS = 5         // stats.media_timeline_record_deferrals
    const val TERMINAL_FATAL = 6
    const val TERMINAL_CODE = 7
    const val COUNT = 8
}

/** The scalar block `nativeReceiverStats` fills: one slot per `moq_media_receiver_stats_t` field, in the struct's order; the two booleans as 0/1. */
internal object ReceiverStatsScalars {
    const val OBJECTS_RECEIVED = 0
    const val OBJECTS_QUEUED = 1
    const val BYTES_QUEUED = 2
    const val OBJECTS_DROPPED = 3
    const val GROUPS_DROPPED = 4
    const val KEYFRAMES_DROPPED = 5
    const val PARSE_DROPS = 6
    const val OVERFLOW_EVENTS = 7
    const val PAUSE_TRANSITIONS = 8
    const val PAUSED = 9
    const val CATALOG_DROPS = 10
    const val CATALOG_COMPLETE = 11
    const val SAP_RECORD_DEFERRALS = 12
    const val MEDIA_TIMELINE_RECORD_DEFERRALS = 13
    const val COUNT = 14
}
