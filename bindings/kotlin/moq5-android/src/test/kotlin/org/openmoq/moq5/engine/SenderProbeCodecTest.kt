package org.openmoq.moq5.engine

import org.openmoq.moq5.SenderFatalCode
import org.openmoq.moq5.SenderStatus
import org.openmoq.moq5.SenderTerminal
import org.openmoq.moq5.UnknownNativeValueException
import org.openmoq.moq5.jni.SenderProbeScalars
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * The decoder from the `nativeSenderProbe` block into [SenderStatus]: the
 * four booleans strict (0/1, anything else fails closed by name), and the
 * terminal derived as the C means it -- `is_fatal` covers sender-fatal OR
 * endpoint-fatal and wins; `is_closed` is the endpoint's closed flag and
 * alone means [SenderTerminal.Closed]. A terminal beats readiness: public
 * `ready` is NORMALISED to false whenever a terminal is present, even if
 * the native `is_ready` bit was 1 (a closed endpoint under a sender that
 * still holds its ready flag), because `write()` refuses in that state and
 * "ready" would be a false "write now".
 */
internal class SenderProbeCodecTest {
    private fun block(ready: Long = 0, closed: Long = 0, fatal: Long = 0, code: Long = 0, demand: Long = 0) = longArrayOf(ready, closed, fatal, code, demand)

    @Test
    fun `the probe decodes into status with the terminal derived as the C means it`() {
        assertEquals(SenderStatus(ready = false, terminal = null, mediaDemand = false), SenderProbeCodec.decode(block()))
        assertEquals(SenderStatus(ready = true, terminal = null, mediaDemand = true), SenderProbeCodec.decode(block(ready = 1, demand = 1)))
        assertEquals(SenderStatus(ready = false, terminal = SenderTerminal.Closed, mediaDemand = false), SenderProbeCodec.decode(block(closed = 1)))
        assertEquals(
            SenderStatus(ready = false, terminal = SenderTerminal.Fatal(SenderFatalCode(0x3uL)), mediaDemand = false),
            SenderProbeCodec.decode(block(fatal = 1, code = 3)),
        )
        assertEquals(SenderTerminal.Fatal(SenderFatalCode(0uL)), SenderProbeCodec.decode(block(closed = 1, fatal = 1, code = 0)).terminal, "fatal wins over closed, even with code 0 (a transport fatal)")
        assertEquals(SenderTerminal.Fatal(SenderFatalCode(ULong.MAX_VALUE)), SenderProbeCodec.decode(block(fatal = 1, code = -1)).terminal, "all 64 bits")
    }

    @Test
    fun `a terminal beats readiness -- ready is normalised to false whenever the block carries a terminal`() {
        assertEquals(SenderStatus(ready = false, terminal = SenderTerminal.Closed, mediaDemand = true), SenderProbeCodec.decode(block(ready = 1, closed = 1, demand = 1)))
        assertEquals(
            SenderStatus(ready = false, terminal = SenderTerminal.Fatal(SenderFatalCode(0x2uL)), mediaDemand = false),
            SenderProbeCodec.decode(block(ready = 1, fatal = 1, code = 2)),
        )
        assertEquals(true, SenderProbeCodec.decode(block(ready = 1)).ready, "without a terminal the native bit is the answer")
    }

    @Test
    fun `the booleans are strict -- only 0 and 1 decode, anything else fails closed by field name`() {
        assertEquals("ready", assertFailsWith<UnknownNativeValueException> { SenderProbeCodec.decode(block(ready = 2)) }.field)
        assertEquals("closed", assertFailsWith<UnknownNativeValueException> { SenderProbeCodec.decode(block(closed = -1)) }.field)
        assertEquals("fatal", assertFailsWith<UnknownNativeValueException> { SenderProbeCodec.decode(block(fatal = 2)) }.field)
        assertEquals("media demand", assertFailsWith<UnknownNativeValueException> { SenderProbeCodec.decode(block(demand = 2)) }.field)
    }

    @Test
    fun `a short block is refused before any field is read`() {
        assertFailsWith<IllegalStateException> { SenderProbeCodec.decode(LongArray(SenderProbeScalars.COUNT - 1)) }
    }
}
