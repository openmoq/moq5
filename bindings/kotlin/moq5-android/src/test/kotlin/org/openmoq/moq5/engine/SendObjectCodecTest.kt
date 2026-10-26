package org.openmoq.moq5.engine

import org.openmoq.moq5.MediaBytes
import org.openmoq.moq5.MediaMicros
import org.openmoq.moq5.SapType
import org.openmoq.moq5.SendObject
import org.openmoq.moq5.UnixMicros
import org.openmoq.moq5.jni.SendObjectScalars
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The encoder to the `nativeSenderWrite` block: booleans as 0/1, the
 * `has_*` flags as the nullability of the Kotlin field, `decode_time_us`
 * the presentation time when the app declared none, `sap_type` the C
 * `moq_sap_type_t` value (NONE=0, TYPE_1..3=1..3).
 */
internal class SendObjectCodecTest {
    @Test
    fun `an object with every optional field absent encodes the C absent conventions`() {
        val sc = SendObjectCodec.encode(SendObject(payload = MediaBytes(byteArrayOf(1)), isSync = true, startsGroup = true, presentationTime = MediaMicros(77uL)))
        assertEquals(SendObjectScalars.COUNT, sc.size)
        assertEquals(listOf(1L, 1L, 0L, 77L, 77L, 0L, 0L, 0L, 0L), sc.toList(), "decode_time_us = presentation; has_capture_time = 0; has_sap_type = 0")
    }

    @Test
    fun `an object with every optional field present encodes them in place, full width`() {
        val obj =
            SendObject(
                payload = MediaBytes(byteArrayOf(1)), isSync = false, startsGroup = false, endsGroup = true,
                presentationTime = MediaMicros(0x8000_0000_0000_0001uL), decodeTime = MediaMicros(ULong.MAX_VALUE),
                captureTime = UnixMicros(1uL shl 40), sapType = SapType.Type3,
            )
        assertEquals(listOf(0L, 0L, 1L, -1L, Long.MIN_VALUE + 1L, 1L, 1L shl 40, 1L, 3L), SendObjectCodec.encode(obj).toList())
        assertEquals(0L, SendObjectCodec.encode(obj.copy(sapType = SapType.None))[SendObjectScalars.SAP_TYPE], "a declared NONE is has_sap_type=1, sap_type=0")
        assertEquals(1L, SendObjectCodec.encode(obj.copy(sapType = SapType.None))[SendObjectScalars.HAS_SAP_TYPE])
    }
}
