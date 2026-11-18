package org.openmoq.moq5

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

/**
 * The receiver-terminal domain: `moq_media_receiver_fatal_code` is the
 * receiver's OWN code (`MOQ_MEDIA_RECEIVER_FATAL_*`), or the endpoint's when
 * the endpoint failed -- it is not a MoQ wire error code and must never be
 * spelt as one. `ReceiverTerminal.Fatal` and `ReceiverFatalException` take a
 * [ReceiverFatalCode] only; that is a compile-time fact these rows pin by
 * constructing them so (a `MoqErrorCode` parameter would not compile them).
 * The JNI carrier is a `jlong` holding all 64 raw bits.
 */
internal class PublicReceiverDomainTest {
    @Test
    fun `the receiver fatal code is its own value type, never equal to a wire error code with the same bits`() {
        val receiver: Any = ReceiverFatalCode(0x2uL)
        val wire: Any = MoqErrorCode(0x2uL)
        assertNotEquals(wire, receiver)
        assertEquals(ReceiverTerminal.Fatal(ReceiverFatalCode(0x2uL)), ReceiverTerminal.Fatal(ReceiverFatalCode(0x2uL)))
        assertEquals(ReceiverFatalCode(0x2uL), ReceiverFatalException(ReceiverFatalCode(0x2uL)).code)
        assertEquals("ReceiverFatalCode(2)", ReceiverFatalCode(0x2uL).toString())
    }

    @Test
    fun `the well-known receiver codes read by name`() {
        assertEquals(ReceiverFatalCode.CATALOG_UNUSABLE, ReceiverFatalCode(0x1uL))
        assertEquals(ReceiverFatalCode.EVENT_OVERFLOW, ReceiverFatalCode(0x2uL))
        assertEquals(ReceiverFatalCode.SETUP_FAILED, ReceiverFatalCode(0x3uL))
        assertEquals(ReceiverFatalCode.CATALOG_REJECTED, ReceiverFatalCode(0x4uL))
    }

    @Test
    fun `a high-bit native value round-trips through the jlong carrier as ULong`() {
        assertEquals(ULong.MAX_VALUE, ReceiverFatalCode.fromNative(-1L).value)
        assertEquals(0x8000_0000_0000_0000uL, ReceiverFatalCode.fromNative(Long.MIN_VALUE).value)
        assertEquals(0x2uL, ReceiverFatalCode.fromNative(2L).value)
        assertTrue(ReceiverFatalCode.fromNative(-1L) != ReceiverFatalCode(0x1uL), "never signed-compared or narrowed")
    }
}
