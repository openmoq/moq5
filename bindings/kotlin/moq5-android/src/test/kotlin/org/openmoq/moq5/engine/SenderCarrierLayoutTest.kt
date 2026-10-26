package org.openmoq.moq5.engine

import org.openmoq.moq5.jni.SendObjectScalars
import org.openmoq.moq5.jni.SendTrackBytes
import org.openmoq.moq5.jni.SendTrackScalars
import org.openmoq.moq5.jni.SenderAttachScalars
import org.openmoq.moq5.jni.SenderProbeScalars
import org.openmoq.moq5.jni.SenderStatsScalars
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The sender carriers, one slot per C field in the C struct's order, with
 * the exact counts the JNI rows refuse a shorter block against.
 */
internal class SenderCarrierLayoutTest {
    @Test
    fun `nativeSenderAttach carries the moq_media_sender_cfg_t scalars in order -- block_timeout_us has no slot`() {
        assertEquals(
            listOf(0, 1, 2, 3, 4, 5, 6, 7, 8),
            listOf(
                SenderAttachScalars.BACKPRESSURE, SenderAttachScalars.QUEUE_MAX_OBJECTS, SenderAttachScalars.QUEUE_MAX_BYTES,
                SenderAttachScalars.PRE_READY_MAX_OBJECTS, SenderAttachScalars.PRE_READY_MAX_BYTES, SenderAttachScalars.VALIDATE_CMAF,
                SenderAttachScalars.PUBLISH_TRACKS, SenderAttachScalars.DROP_WITHOUT_DEMAND, SenderAttachScalars.CATALOG_REFRESH_INTERVAL_US,
            ),
        )
        assertEquals(9, SenderAttachScalars.COUNT)
        assertEquals(1, SenderAttachScalars.OUT_COUNT, "the owned child token")
    }

    @Test
    fun `nativeSenderAddTrack carries six byte fields and the moq_media_track_cfg_t scalars in order`() {
        assertEquals(
            listOf(0, 1, 2, 3, 4, 5),
            listOf(SendTrackBytes.NAME, SendTrackBytes.CODEC, SendTrackBytes.INIT_DATA, SendTrackBytes.ROLE, SendTrackBytes.LANG, SendTrackBytes.CHANNEL_CONFIG),
        )
        assertEquals(6, SendTrackBytes.COUNT)
        assertEquals(
            (0..20).toList(),
            listOf(
                SendTrackScalars.MEDIA_TYPE, SendTrackScalars.PACKAGING, SendTrackScalars.TIMESCALE, SendTrackScalars.IS_LIVE,
                SendTrackScalars.WIDTH, SendTrackScalars.HEIGHT, SendTrackScalars.FRAMERATE_MILLIS, SendTrackScalars.SAMPLERATE,
                SendTrackScalars.BITRATE, SendTrackScalars.HAS_MAX_GRP_SAP, SendTrackScalars.MAX_GRP_SAP, SendTrackScalars.HAS_MAX_OBJ_SAP,
                SendTrackScalars.MAX_OBJ_SAP, SendTrackScalars.EMIT_SAP_TIMELINE, SendTrackScalars.SAP_TIMELINE_HISTORY_GROUPS,
                SendTrackScalars.HAS_TRACK_DURATION, SendTrackScalars.TRACK_DURATION_MS, SendTrackScalars.EMIT_MEDIA_TIMELINE,
                SendTrackScalars.MEDIA_TIMELINE_HISTORY_GROUPS, SendTrackScalars.HAS_ALT_GROUP, SendTrackScalars.ALT_GROUP,
            ),
        )
        assertEquals(21, SendTrackScalars.COUNT)
        assertEquals(1, SendTrackScalars.OUT_COUNT, "the borrowed track token")
    }

    @Test
    fun `nativeSenderWrite carries the moq_media_send_object_t scalars in order`() {
        assertEquals(
            (0..8).toList(),
            listOf(
                SendObjectScalars.IS_SYNC, SendObjectScalars.STARTS_GROUP, SendObjectScalars.ENDS_GROUP, SendObjectScalars.DECODE_TIME_US,
                SendObjectScalars.PRESENTATION_TIME_US, SendObjectScalars.HAS_CAPTURE_TIME, SendObjectScalars.CAPTURE_TIME_US,
                SendObjectScalars.HAS_SAP_TYPE, SendObjectScalars.SAP_TYPE,
            ),
        )
        assertEquals(9, SendObjectScalars.COUNT)
    }

    @Test
    fun `nativeSenderProbe publishes the five state queries in order`() {
        assertEquals(
            (0..4).toList(),
            listOf(SenderProbeScalars.READY, SenderProbeScalars.CLOSED, SenderProbeScalars.FATAL, SenderProbeScalars.FATAL_CODE, SenderProbeScalars.MEDIA_DEMAND),
        )
        assertEquals(5, SenderProbeScalars.COUNT)
    }

    @Test
    fun `nativeSenderStats publishes the moq_media_sender_stats_t fields in order`() {
        assertEquals(
            (0..10).toList(),
            listOf(
                SenderStatsScalars.OBJECTS_WRITTEN, SenderStatsScalars.OBJECTS_SENT, SenderStatsScalars.OBJECTS_QUEUED, SenderStatsScalars.BYTES_QUEUED,
                SenderStatsScalars.OBJECTS_DROPPED, SenderStatsScalars.GROUPS_DROPPED, SenderStatsScalars.KEYFRAMES_DROPPED, SenderStatsScalars.GROUPS_ABANDONED,
                SenderStatsScalars.BACKPRESSURE_STALLS, SenderStatsScalars.LAST_ERROR, SenderStatsScalars.SAP_RECORDS_EVICTED,
            ),
        )
        assertEquals(11, SenderStatsScalars.COUNT)
    }
}
