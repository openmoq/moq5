package org.openmoq.moq5.engine

import org.openmoq.moq5.MediaMillis
import org.openmoq.moq5.ObjectLocation
import org.openmoq.moq5.SapType
import org.openmoq.moq5.UnixMillis
import org.openmoq.moq5.jni.ReceiverSapScalars
import org.openmoq.moq5.jni.ReceiverTimelineScalars

/**
 * The decoders from the record rows' scalar blocks. [sapTypeOf] maps only
 * NONE/1/2/3 and fails closed by field name on anything else
 * (`MOQ_SAP_UNKNOWN` included); ids and times keep all 64 bits; the
 * timeline's `wallclock_ms` maps through `UnixMillis.fromNativeOrNull`
 * (zero = absent) here, at the boundary.
 */
internal object TimelineRecordCodec {
    fun sapTypeOf(raw: Long): SapType {
        if (raw < 0L || raw > 3L) throw org.openmoq.moq5.UnknownNativeValueException("sap type", raw)
        return SapType.ofNative(raw.toInt())
    }

    fun decodeSap(sc: LongArray): BackendSapRecord =
        BackendSapRecord(
            track = sc[ReceiverSapScalars.TRACK],
            location = ObjectLocation(sc[ReceiverSapScalars.GROUP].toULong(), sc[ReceiverSapScalars.OBJECT].toULong()),
            sapType = sapTypeOf(sc[ReceiverSapScalars.SAP_TYPE]),
            earliestPresentationTime = MediaMillis(sc[ReceiverSapScalars.EPT_MS].toULong()),
        )

    fun decodeTimeline(sc: LongArray): BackendTimelineRecord =
        BackendTimelineRecord(
            track = sc[ReceiverTimelineScalars.TRACK],
            mediaTime = MediaMillis(sc[ReceiverTimelineScalars.MEDIA_TIME_MS].toULong()),
            location = ObjectLocation(sc[ReceiverTimelineScalars.GROUP].toULong(), sc[ReceiverTimelineScalars.OBJECT].toULong()),
            wallclock = UnixMillis.fromNativeOrNull(sc[ReceiverTimelineScalars.WALLCLOCK_MS].toULong()),
        )
}
