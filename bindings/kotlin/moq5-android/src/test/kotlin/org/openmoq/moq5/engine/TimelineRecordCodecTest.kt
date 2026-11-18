package org.openmoq.moq5.engine

import org.openmoq.moq5.MediaMillis
import org.openmoq.moq5.ObjectLocation
import org.openmoq.moq5.SapType
import org.openmoq.moq5.UnixMillis
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.ReceiverSapScalars
import org.openmoq.moq5.jni.ReceiverTimelineScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The decoders from the `nativeReceiverPollSap` / `nativeReceiverPollMediaTimeline`
 * scalar blocks. `moq_sap_type_t` maps only onto NONE=0, TYPE_1=1, TYPE_2=2,
 * TYPE_3=3 (`media/cmaf/include/moq/cmaf.h`); `MOQ_SAP_UNKNOWN` (0xFF) and
 * anything else fail closed by field name. 64-bit ids and times keep every
 * bit; `wallclock_ms` zero is absent.
 */
internal class TimelineRecordCodecTest {
    @Test
    fun `sap type maps the closed CMSF set and fails closed on UNKNOWN and anything else`() {
        assertEquals(listOf(SapType.None, SapType.Type1, SapType.Type2, SapType.Type3), (0L..3L).map { TimelineRecordCodec.sapTypeOf(it) })
        for (raw in listOf(4L, 0xFFL, -1L, Long.MIN_VALUE)) {
            assertEquals("sap type", assertFailsWith<UnknownNativeValueException> { TimelineRecordCodec.sapTypeOf(raw) }.field, "value $raw")
        }
    }

    @Test
    fun `a sap block decodes with full widths and millisecond domain`() {
        val sc = LongArray(ReceiverSapScalars.COUNT)
        sc[ReceiverSapScalars.TRACK] = 7L
        sc[ReceiverSapScalars.GROUP] = -1L
        sc[ReceiverSapScalars.OBJECT] = Long.MIN_VALUE
        sc[ReceiverSapScalars.SAP_TYPE] = 3L
        sc[ReceiverSapScalars.EPT_MS] = -2L
        val r = TimelineRecordCodec.decodeSap(sc)
        assertEquals(7L, r.track)
        assertEquals(ObjectLocation(groupId = ULong.MAX_VALUE, objectId = 0x8000_0000_0000_0000uL), r.location, "the OBJECT slot mirrors the C field; the value is a location")
        assertEquals(SapType.Type3, r.sapType)
        assertEquals(MediaMillis(ULong.MAX_VALUE - 1uL), r.earliestPresentationTime)
    }

    @Test
    fun `a timeline block decodes with full widths, millisecond media time, and an absent wallclock for zero`() {
        val sc = LongArray(ReceiverTimelineScalars.COUNT)
        sc[ReceiverTimelineScalars.TRACK] = 9L
        sc[ReceiverTimelineScalars.MEDIA_TIME_MS] = -1L
        sc[ReceiverTimelineScalars.GROUP] = 1L
        sc[ReceiverTimelineScalars.OBJECT] = 2L
        sc[ReceiverTimelineScalars.WALLCLOCK_MS] = 0L
        val r = TimelineRecordCodec.decodeTimeline(sc)
        assertEquals(MediaMillis(ULong.MAX_VALUE), r.mediaTime)
        assertEquals(ObjectLocation(1uL, 2uL), r.location)
        assertEquals(null, r.wallclock)
        sc[ReceiverTimelineScalars.WALLCLOCK_MS] = 1_700_000_000_000L
        assertEquals(UnixMillis(1_700_000_000_000uL), TimelineRecordCodec.decodeTimeline(sc).wallclock)
    }
}
